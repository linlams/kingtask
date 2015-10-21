//process
package broker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/flike/golog"

	redis "gopkg.in/redis.v3"
	"github.com/flike/kingtask/config"
	"github.com/flike/kingtask/core/errors"
	"github.com/flike/kingtask/core/timer"
	"github.com/flike/kingtask/task"
)

type Broker struct {
	cfg         *config.BrokerConfig
	addr        string
	redisAddr   string
	redisDB     int
	running     bool
	listener    net.Listener
	redisClient *redis.Client
	timer       *timer.Timer
}

func NewBroker(cfg *config.BrokerConfig) (*Broker, error) {
	var err error

	broker := new(Broker)
	broker.cfg = cfg
	broker.addr = cfg.Addr

	vec := strings.SplitN(cfg.RedisAddr, "/", 2)
	if len(vec) == 2 {
		broker.redisAddr = vec[0]
		broker.redisDB, err = strconv.Atoi(vec[1])
		if err != nil {
			return nil, err
		}
	} else {
		broker.redisAddr = vec[0]
		broker.redisDB = config.DefaultRedisDB
	}

	broker.listener, err = net.Listen("tcp", broker.addr)
	if err != nil {
		return nil, err
	}

	broker.timer = timer.New(time.Millisecond * 10)
	go broker.timer.Start()

	broker.redisClient = redis.NewClient(
		&redis.Options{
			Addr:     broker.redisAddr,
			Password: "", // no password set
			DB:       int64(broker.redisDB),
		},
	)
	_, err = broker.redisClient.Ping().Result()
	if err != nil {
		golog.Error("broker", "NewBroker", "ping redis fail", 0, "err", err.Error())
		return nil, err
	}

	return broker, nil
}

func (b *Broker) Run() error {
	b.running = true

	go b.HandleFailTask()
	for b.running {
		conn, err := b.listener.Accept()
		if err != nil {
			golog.Error("server", "Run", err.Error(), 0)
			continue
		}
		//处理客户端请求
		go b.handleConn(conn)
	}

	return nil
}

func (b *Broker) Close() {
	b.running = false
	if b.listener != nil {
		b.listener.Close()
	}
	b.redisClient.Close()
	b.timer.Stop()
}

func (b *Broker) WriteError(err error, c net.Conn) error {
	var result task.StatusResult
	if err == nil {
		return nil
	}

	result.Status = 1
	result.Message = err.Error()
	ret, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = c.Write(ret)
	if err != nil {
		return err
	}
	return nil
}

func (b *Broker) WriteOK(c net.Conn) error {
	var result task.StatusResult

	result.Status = 0
	ret, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = c.Write(ret)
	if err != nil {
		return err
	}
	return nil
}

func (b *Broker) WriteResult(status int, isSuccess string, r string, c net.Conn) error {
	var result task.Reply
	var err error

	result.IsResultExist = status
	result.IsSuccess, err = strconv.Atoi(isSuccess)
	if err != nil {
		b.WriteError(err, c)
		return err
	}
	result.Result = r

	ret, err := json.Marshal(result)
	if err != nil {
		b.WriteError(err, c)
		return err
	}
	_, err = c.Write(ret)
	if err != nil {
		return err
	}
	return nil
}

func (b *Broker) handleConn(c net.Conn) error {
	defer func() {
		r := recover()
		if err, ok := r.(error); ok {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]

			golog.Error("Broker", "handleConn",
				err.Error(), 0,
				"stack", string(buf))
		}
		c.Close()
	}()

	var CloseConn bool
	reader := bufio.NewReaderSize(c, 1024)
	for {
		msgType := []byte{0}
		if _, err := io.ReadFull(reader, msgType); err != nil {
			return errors.ErrBadConn
		}

		switch msgType[0] {
		case config.TypeRequestTask:
			b.HandleRequest(reader, c)
		case config.TypeGetTaskResult:
			b.HandleTaskResult(reader, c)
		case config.TypeCloseConn:
			CloseConn = true
		default:
			golog.Error("Broker", "handleConn", "msgType error", 0, "msg_type", msgType[0])
			CloseConn = true
		}
		if CloseConn {
			break
		}
	}
	return nil
}

func (b *Broker) HandleTaskResult(rb *bufio.Reader, c net.Conn) error {
	args := struct {
		Key string `json:"key"`
	}{}
	buf := make([]byte, 128)
	readLen, err := rb.Read(buf)
	if err != nil {
		b.WriteError(err, c)
		return err
	}

	err = json.Unmarshal(buf[:readLen], &args)
	if err != nil {
		b.WriteError(err, c)
		return err
	}

	result, err := b.redisClient.HMGet(args.Key,
		"is_success",
		"result",
	).Result()
	if err != nil {
		golog.Error("Broker", "HandleFailTask", err.Error(), 0, "key", args.Key)
		b.WriteError(err, c)
		return err
	}

	//key不存在
	if result[0] == nil {
		err = b.WriteResult(config.ResultNotExist, "0", "", c)
		return err
	}
	isSuccess := result[0].(string)
	ret := result[1].(string)
	return b.WriteResult(config.ResultIsExist, isSuccess, ret, c)
}

func (b *Broker) HandleRequest(rb *bufio.Reader, c net.Conn) error {
	buf := make([]byte, 1024)
	request := new(task.TaskRequest)
	readLen, err := rb.Read(buf)
	if err != nil {
		b.WriteError(err, c)
		return err
	}

	err = json.Unmarshal(buf[:readLen], request)
	if err != nil {
		b.WriteError(err, c)
		return err
	}

	now := time.Now().Unix()
	if request.StartTime == 0 {
		request.StartTime = now
	}

	if request.StartTime <= now {
		err = b.AddRequestToRedis(request)
		if err != nil {
			b.WriteError(err, c)
			return err
		}
	} else {
		afterTime := time.Second * time.Duration(request.StartTime-now)
		b.timer.NewTimer(afterTime, b.AddRequestToRedis, request)
	}

	return b.WriteOK(c)
}

//处理失败的任务
func (b *Broker) HandleFailTask() error {
	var uuid string
	var err error
	for b.running {
		uuid, err = b.redisClient.SPop(config.FailResultUuidSet).Result()
		//没有结果，直接返回
		if err == redis.Nil {
			time.Sleep(time.Second)
			continue
		}
		if err != nil {
			golog.Error("Broker", "HandleResult", "spop error", 0, "error", err.Error())
			continue
		}

		key := fmt.Sprintf("r_%s", uuid)
		timeInterval, err := b.redisClient.HGet(key, "time_interval").Result()
		if err != nil {
			golog.Error("Broker", "HandleFailTask", err.Error(), 0, "key", key)
			continue
		}
		//没有超时重试机制
		if len(timeInterval) == 0 {
			continue
		}
		//获取结果中所有值,改为逐个获取
		results, err := b.redisClient.HMGet(key,
			"uuid",
			"bin_name",
			"args",
			"start_time",
			"time_interval",
			"index",
		).Result()
		if err != nil {
			golog.Error("Broker", "HandleFailTask", err.Error(), 0, "key", key)
			continue
		}
		//key已经过期
		if results[0] == nil {
			golog.Error("Broker", "HandleFailTask", "result expired", 0, "key", key)
			continue
		}
		//删除结果
		_, err = b.redisClient.Del(key).Result()
		if err != nil {
			golog.Error("Broker", "HandleFailTask", "delete result failed", 0, "key", key)
		}
		err = b.resetTaskRequest(results)
		if err != nil {
			golog.Error("Broker", "HandleFailTask", err.Error(), 0, "key", key)
		}
	}

	return nil
}

func (b *Broker) resetTaskRequest(args []interface{}) error {
	var err error
	if len(args) == 0 || len(args) != config.TaskRequestItemCount {
		return errors.ErrInvalidArgument
	}
	request := new(task.TaskRequest)
	request.Uuid = args[0].(string)
	request.BinName = args[1].(string)
	request.Args = args[2].(string)
	request.StartTime, err = strconv.ParseInt(args[3].(string), 10, 64)
	if err != nil {
		return err
	}
	request.TimeInterval = args[4].(string)
	request.Index, err = strconv.Atoi(args[5].(string))
	if err != nil {
		return err
	}
	vec := strings.Split(request.TimeInterval, " ")
	request.Index++
	if request.Index < len(vec) {
		timeLater, err := strconv.Atoi(vec[request.Index])
		if err != nil {
			return err
		}
		afterTime := time.Second * time.Duration(timeLater)
		b.timer.NewTimer(afterTime, b.AddRequestToRedis, request)
	} else {
		golog.Error("Broker", "HandleFailTask", "retry max time", 0,
			"key", fmt.Sprintf("t_%s", request.Uuid))
		return errors.ErrTryMaxTimes
	}
	return nil
}

func (b *Broker) AddRequestToRedis(tr interface{}) error {
	r, ok := tr.(*task.TaskRequest)
	if !ok {
		return errors.ErrInvalidArgument
	}
	key := fmt.Sprintf("t_%s", r.Uuid)
	setCmd := b.redisClient.HMSet(key,
		"uuid", r.Uuid,
		"bin_name", r.BinName,
		"args", r.Args,
		"start_time", strconv.FormatInt(r.StartTime, 10),
		"time_interval", r.TimeInterval,
		"index", strconv.Itoa(r.Index),
	)
	err := setCmd.Err()
	if err != nil {
		golog.Error("Broker", "AddRequestToRedis", "HMSET error", 0,
			"set", config.RequestUuidSet,
			"uuid", r.Uuid,
			"err", err.Error(),
		)
		return err
	}
	saddCmd := b.redisClient.SAdd(config.RequestUuidSet, r.Uuid)
	err = saddCmd.Err()
	if err != nil {
		golog.Error("Broker", "AddRequestToRedis", "SADD error", 0,
			"set", config.RequestUuidSet,
			"uuid", r.Uuid,
			"err", err.Error(),
		)
		return err
	}

	return nil
}
