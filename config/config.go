package config

import (
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

type BrokerConfig struct {
	Addr      string `yaml:"addr"`
	RedisAddr string `yaml:"redis"`
	LogPath   string `yaml:"log_path"`
	LogLevel  string `yaml:"log_level"`
}

type WorkerConfig struct {
	BrokerAddr     string `yaml:"broker"`
	RedisAddr      string `yaml:"redis"`
	LogPath        string `yaml:"log_path"`
	LogLevel       string `yaml:"log_level"`
	BinPath        string `yaml:"bin_path"`
	Peroid         int64  `yaml:"peroid"`
	ResultKeepTime int64  `yaml:"result_keep_time"`
	TaskRunTime    int64  `yaml:"task_run_time"`
}

func ParseBrokerConfigFile(fileName string) (*BrokerConfig, error) {
	var cfg BrokerConfig

	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func ParseWorkerConfigFile(fileName string) (*WorkerConfig, error) {
	var cfg WorkerConfig

	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
