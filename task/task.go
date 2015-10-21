package task

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/pborman/uuid"
	"github.com/flike/kingtask/config"
	"github.com/flike/kingtask/core/errors"
)

type BrokerClient struct {
	BrokerAddr string
	BrokerConn *net.TCPConn
}

type TaskRequest struct {
	Uuid         string `json:"uuid"`
	BinName      string `json:"bin_name"`
	Args         string `json:"args"` //空格分隔各个参数
	StartTime    int64  `json:"start_time"`
	TimeInterval string `json:"time_interval"` //空格分隔各个参数
	Index        int    `json:"index"`
}

type TaskResult struct {
	TaskRequest
	IsSuccess int64  `json:"is_success"`
	Result    string `json:"result"`
}

type StatusResult struct {
	Status  int    `json:"status"`
	Message string `json:"message"`
}

type Reply struct {
	IsResultExist int    `json:"is_result_exist"`
	IsSuccess     int    `json:"is_success"`
	Result        string `json:"message"`
}

func NewTaskRequest(binName string, args []string, startTime int64, timeInterval []int) (*TaskRequest, error) {
	if len(binName) == 0 {
		return nil, errors.ErrInvalidArgument
	}

	taskRequest := new(TaskRequest)
	taskRequest.Uuid = uuid.New()
	taskRequest.BinName = binName
	if len(args) != 0 {
		taskRequest.Args = strings.Join(args, " ")
	}
	timeIntervalLen := len(timeInterval)
	if timeIntervalLen != 0 {
		strVec := make([]string, timeIntervalLen)
		for i := 0; i < timeIntervalLen; i++ {
			strVec[i] = strconv.Itoa(timeInterval[i])
		}
		taskRequest.TimeInterval = strings.Join(strVec, " ")
	}
	if startTime == 0 {
		taskRequest.StartTime = time.Now().Unix()
	} else {
		taskRequest.StartTime = startTime
	}

	return taskRequest, nil
}

func NewBrokerClient(brokerAddr string) (*BrokerClient, error) {
	if len(brokerAddr) == 0 {
		return nil, errors.ErrInvalidArgument
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", brokerAddr)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return nil, err
	}
	err = conn.SetKeepAlive(true)
	if err != nil {
		return nil, err
	}

	return &BrokerClient{
		BrokerAddr: brokerAddr,
		BrokerConn: conn,
	}, nil
}

func (k *BrokerClient) Delay(t *TaskRequest) error {
	if t == nil {
		return nil
	}
	reply := make([]byte, 512)
	result := new(StatusResult)

	ret, err := json.Marshal(t)
	if err != nil {
		fmt.Printf("mashal error:%s\n", err.Error())
		return err
	}
	sendBuf := make([]byte, len(ret)+1)
	sendBuf[0] = config.TypeRequestTask
	copy(sendBuf[1:], ret)
	_, err = k.BrokerConn.Write(sendBuf)
	if err != nil {
		fmt.Printf("Write error:%s\n", err.Error())
		return err
	}
	readLen, err := k.BrokerConn.Read(reply)
	if err != nil {
		fmt.Printf("Read error:%s\n", err.Error())
		return err
	}
	err = json.Unmarshal(reply[:readLen], result)
	if err != nil {
		fmt.Printf("Unmarshal error:%s\n", err.Error())
		return err
	}
	if result.Status == 1 {
		return errors.NewError(result.Message)
	}

	return nil
}

func (k *BrokerClient) GetResult(t *TaskRequest) (*Reply, error) {
	reply := make([]byte, 512)
	result := new(Reply)
	args := struct {
		Key string `json:"key"`
	}{}

	args.Key = fmt.Sprintf("r_%s", t.Uuid)
	buf, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	sendBuf := make([]byte, len(buf)+1)
	sendBuf[0] = config.TypeGetTaskResult
	copy(sendBuf[1:], buf)

	_, err = k.BrokerConn.Write(sendBuf)
	if err != nil {
		return nil, err
	}
	readLen, err := k.BrokerConn.Read(reply)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(reply[:readLen], result)
	if err != nil {
		return nil, err
	}
	if result.IsResultExist == config.ResultNotExist {
		return nil, errors.ErrResultNotReady
	}

	return result, nil
}

func (k *BrokerClient) Close() error {
	header := []byte{config.TypeCloseConn}
	_, err := k.BrokerConn.Write(header)
	if err != nil {
		return err
	}
	err = k.BrokerConn.Close()
	return err
}
