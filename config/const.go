package config

const (
	DefaultRedisDB       = 0
	TaskRequestItemCount = 6
	RequestUuidSet       = "request_uuid_set"
	FailResultUuidSet    = "fail_result_uuid_set"
	TypeRequestTask      = 1
	TypeGetTaskResult    = 2
	TypeCloseConn        = 3
)

const (
	ResultNotExist = 0
	ResultIsExist  = 1
)
