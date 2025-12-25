package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/smtp"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
	"github.com/shirou/gopsutil/v3/process"

	_ "modernc.org/sqlite"
)

// --- 全局变量 ---
var (
	latestMetric Metric
	db           *sql.DB
	metricMutex  sync.RWMutex

	// 配置对象及其锁
	appConfig   Config
	configMutex sync.RWMutex

	// 报警状态记录
	alertStates      = make(map[string]*AlertState)
	alertStatesMutex sync.Mutex

	// 日志对象
	runLogger  *log.Logger // 运行日志
	sendLogger *log.Logger // 发送日志

	//go:embed index.html
	staticFiles embed.FS
)

// 报警状态
type AlertState struct {
	IsFiring      bool
	LastAlertTime int64
}

// --- 配置结构体 ---

type AlertConfig struct {
	Enable          bool    `json:"enable"`
	EnableRecovery  bool    `json:"enable_recovery"`
	CpuThreshold    float64 `json:"cpu_threshold"`
	MemThreshold    float64 `json:"mem_threshold"`
	DiskThreshold   float64 `json:"disk_threshold"`
	CooldownSeconds int64   `json:"cooldown_seconds"`
	TriggerTemplate string  `json:"trigger_template"`
	RecoverTemplate string  `json:"recover_template"`
}

type NotifyConfig struct {
	EnableEmail    bool   `json:"enable_email"`
	EmailHost      string `json:"email_host"`
	EmailPort      int    `json:"email_port"`
	EmailUser      string `json:"email_user"`
	EmailPass      string `json:"email_pass"`
	EmailTo        string `json:"email_to"`
	EnableDingTalk bool   `json:"enable_dingtalk"`
	DingTalkToken  string `json:"dingtalk_token"`
	EnableFeishu   bool   `json:"enable_feishu"`
	FeishuWebhook  string `json:"feishu_webhook"`
}

type Config struct {
	AppName       string       `json:"app_name"`
	RetentionDays int          `json:"retention_days"`
	ServerPort    int          `json:"server_port"`
	DBPath        string       `json:"db_path"`
	RunLogPath    string       `json:"run_log_path"`
	SendLogPath   string       `json:"send_log_path"`
	Alert         AlertConfig  `json:"alert"`
	Notify        NotifyConfig `json:"notify"`
}

// --- 监控指标结构体 ---
type Metric struct {
	Timestamp        int64          `json:"timestamp"`
	Hostname         string         `json:"hostname"`
	OS               string         `json:"os"`
	Kernel           string         `json:"kernel"`
	Arch             string         `json:"arch"`
	Uptime           uint64         `json:"uptime_seconds"`
	BootTime         uint64         `json:"boot_time"`
	ProcessCount     uint64         `json:"process_count"`
	Load1            float64        `json:"load_1"`
	Load5            float64        `json:"load_5"`
	Load15           float64        `json:"load_15"`
	CpuModel         string         `json:"cpu_model"`
	CpuPhysicalCores int            `json:"cpu_physical_cores"`
	CpuLogicalCores  int            `json:"cpu_logical_cores"`
	CpuPercent       float64        `json:"cpu_percent"`
	CpuUserPercent   float64        `json:"cpu_user_percent"`
	CpuSystemPercent float64        `json:"cpu_system_percent"`
	CpuIowaitPercent float64        `json:"cpu_iowait_percent"`
	CpuIdlePercent   float64        `json:"cpu_idle_percent"`
	MemTotal         uint64         `json:"mem_total_mb"`
	MemUsed          uint64         `json:"mem_used_mb"`
	MemFree          uint64         `json:"mem_free_mb"`
	MemBuffers       uint64         `json:"mem_buffers_mb"`
	MemCached        uint64         `json:"mem_cached_mb"`
	MemPercent       float64        `json:"mem_percent"`
	SwapTotal        uint64         `json:"swap_total_mb"`
	SwapUsed         uint64         `json:"swap_used_mb"`
	SwapPercent      float64        `json:"swap_percent"`
	Disks            []DiskInfo     `json:"disks"`
	DiskReadSpeed    float64        `json:"disk_read_kb_s"`
	DiskWriteSpeed   float64        `json:"disk_write_kb_s"`
	DiskIopsRead     float64        `json:"disk_read_iops"`
	DiskIopsWrite    float64        `json:"disk_write_iops"`
	NetRxSpeed       float64        `json:"net_rx_kb_s"`
	NetTxSpeed       float64        `json:"net_tx_kb_s"`
	NetPacketsRecv   float64        `json:"net_packets_rx_s"`
	NetPacketsSent   float64        `json:"net_packets_tx_s"`
	ListeningPorts   []PortInfo     `json:"listening_ports"`
	TcpStateCounts   map[string]int `json:"tcp_state_counts"`
	ArpTable         []ArpEntry     `json:"arp_table"`
	TopProcesses     []ProcessInfo  `json:"top_processes"` // 新增：Top进程列表
}

type DiskInfo struct {
	Path              string  `json:"path"`
	Fstype            string  `json:"fstype"`
	TotalGB           float64 `json:"total_gb"`
	UsedGB            float64 `json:"used_gb"`
	UsedPercent       float64 `json:"used_percent"`
	InodesUsedPercent float64 `json:"inodes_used_percent"`
}

type ArpEntry struct {
	IPAddress string `json:"ip_address"`
	HWAddress string `json:"hw_address"`
	Device    string `json:"device"`
}

type PortInfo struct {
	Port        int     `json:"port"`
	PID         int32   `json:"pid"`
	ProcessName string  `json:"process_name"`
	CpuPercent  float64 `json:"cpu_percent"`
	MemPercent  float64 `json:"mem_percent"`
	User        string  `json:"user"` // 新增：端口对应进程的用户
}

// 新增：进程详细信息结构
type ProcessInfo struct {
	PID        int32   `json:"pid"`
	Name       string  `json:"name"`
	User       string  `json:"user"`
	Status     string  `json:"status"` // R:Running, S:Sleep...
	CpuPercent float64 `json:"cpu_percent"`
	MemPercent float64 `json:"mem_percent"`
	MemRSS     uint64  `json:"mem_rss_mb"` // 实际物理内存
}

func main() {
	// 1. 加载配置
	loadConfig()

	// 2. 初始化日志系统
	initLoggers()

	if runtime.GOOS != "linux" {
		runLogger.Println("提示: 建议在 Linux 环境下以 root 权限运行。")
	}

	// 3. 初始化数据库
	initDB()
	defer db.Close()

	runLogger.Println("-------------------------------------------")
	runLogger.Printf("   %s (运行中)\n", appConfig.AppName)
	runLogger.Println("-------------------------------------------")
	runLogger.Printf("1. 数据库路径: %s\n", appConfig.DBPath)
	runLogger.Printf("2. 服务端口: %d\n", appConfig.ServerPort)
	runLogger.Printf("3. 数据保留: %d 天\n", appConfig.RetentionDays)
	runLogger.Printf("4. 运行日志: %s\n", appConfig.RunLogPath)
	runLogger.Printf("5. 报警日志: %s\n", appConfig.SendLogPath)
	runLogger.Printf("6. 访问地址: http://localhost:%d\n", appConfig.ServerPort)
	runLogger.Println("-------------------------------------------")

	go startRetentionPolicy()
	go startCollector()
	startWebServer()
}

func loadConfig() {
	configFile := "config.json"
	defaultConfig := Config{
		AppName:       "Pengfei监控探针",
		RetentionDays: 30,
		ServerPort:    8080,
		DBPath:        "monitor.db",
		RunLogPath:    "run.log",
		SendLogPath:   "send.log",
		Alert: AlertConfig{
			Enable:          false,
			EnableRecovery:  true,
			CpuThreshold:    80.0,
			MemThreshold:    80.0,
			DiskThreshold:   85.0,
			CooldownSeconds: 300,
			TriggerTemplate: "【报警】\n主机: {{hostname}}\n时间: {{time}}\n内容: {{type}} 使用率过高: {{value}}% (阈值: {{threshold}}%)",
			RecoverTemplate: "【恢复】\n主机: {{hostname}}\n时间: {{time}}\n内容: {{type}} 已恢复正常: {{value}}% (阈值: {{threshold}}%)",
		},
		Notify: NotifyConfig{
			EnableEmail:    false,
			EmailHost:      "smtp.qq.com",
			EmailPort:      587,
			EnableDingTalk: false,
			EnableFeishu:   false,
		},
	}

	configMutex.Lock()
	defer configMutex.Unlock()

	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		data, err := json.MarshalIndent(defaultConfig, "", "  ")
		if err == nil {
			_ = os.WriteFile(configFile, data, 0644)
			fmt.Println("未找到配置文件，已生成默认 config.json")
		}
		appConfig = defaultConfig
	} else {
		data, err := os.ReadFile(configFile)
		if err == nil {
			if err := json.Unmarshal(data, &appConfig); err == nil {
				fmt.Println("已加载配置文件 config.json")
				// 简单的默认值回填
				if appConfig.AppName == "" {
					appConfig.AppName = defaultConfig.AppName
				}
				if appConfig.RetentionDays <= 0 {
					appConfig.RetentionDays = defaultConfig.RetentionDays
				}
				if appConfig.RunLogPath == "" {
					appConfig.RunLogPath = defaultConfig.RunLogPath
				}
				if appConfig.SendLogPath == "" {
					appConfig.SendLogPath = defaultConfig.SendLogPath
				}
				if appConfig.Alert.TriggerTemplate == "" {
					appConfig.Alert.TriggerTemplate = defaultConfig.Alert.TriggerTemplate
				}
				if appConfig.Alert.RecoverTemplate == "" {
					appConfig.Alert.RecoverTemplate = defaultConfig.Alert.RecoverTemplate
				}
			} else {
				log.Printf("配置文件解析失败，使用默认配置: %v", err)
				appConfig = defaultConfig
			}
		} else {
			appConfig = defaultConfig
		}
	}
}

// 初始化日志系统
func initLoggers() {
	// 1. 初始化运行日志
	runFile, err := os.OpenFile(appConfig.RunLogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("无法打开运行日志文件 (%s): %v", appConfig.RunLogPath, err)
	}
	runLogger = log.New(io.MultiWriter(os.Stdout, runFile), "[RUN] ", log.Ldate|log.Ltime)

	// 2. 初始化发送日志
	sendFile, err := os.OpenFile(appConfig.SendLogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("无法打开报警日志文件 (%s): %v", appConfig.SendLogPath, err)
	}
	sendLogger = log.New(io.MultiWriter(os.Stdout, sendFile), "[SEND] ", log.Ldate|log.Ltime)
}

func saveConfigToFile() error {
	configMutex.RLock()
	data, err := json.MarshalIndent(appConfig, "", "  ")
	configMutex.RUnlock()

	if err != nil {
		return err
	}
	return os.WriteFile("config.json", data, 0644)
}

// --- 报警核心逻辑 ---

func checkAlerts(m Metric) {
	configMutex.RLock()
	alertCfg := appConfig.Alert
	configMutex.RUnlock()

	if !alertCfg.Enable {
		return
	}

	processMetricAlert("cpu", "CPU", m.CpuPercent, alertCfg.CpuThreshold, alertCfg)
	processMetricAlert("mem", "内存", m.MemPercent, alertCfg.MemThreshold, alertCfg)
	for _, d := range m.Disks {
		key := fmt.Sprintf("disk:%s", d.Path)
		name := fmt.Sprintf("磁盘[%s]", d.Path)
		processMetricAlert(key, name, d.UsedPercent, alertCfg.DiskThreshold, alertCfg)
	}
}

func processMetricAlert(key string, name string, value float64, threshold float64, cfg AlertConfig) {
	alertStatesMutex.Lock()
	state, exists := alertStates[key]
	if !exists {
		state = &AlertState{IsFiring: false, LastAlertTime: 0}
		alertStates[key] = state
	}

	now := time.Now().Unix()

	if value > threshold {
		if !state.IsFiring || (now-state.LastAlertTime) >= cfg.CooldownSeconds {
			state.IsFiring = true
			state.LastAlertTime = now
			alertStatesMutex.Unlock()

			msg := renderTemplate(cfg.TriggerTemplate, name, value, threshold)
			sendLogger.Printf("触发报警: %s", strings.ReplaceAll(msg, "\n", " "))
			go sendNotifications(msg)
			return
		}
	} else {
		if state.IsFiring {
			state.IsFiring = false
			alertStatesMutex.Unlock()

			if cfg.EnableRecovery {
				msg := renderTemplate(cfg.RecoverTemplate, name, value, threshold)
				sendLogger.Printf("触发恢复: %s", strings.ReplaceAll(msg, "\n", " "))
				go sendNotifications(msg)
			}
			return
		}
	}

	alertStatesMutex.Unlock()
}

func renderTemplate(tpl string, typeName string, value float64, threshold float64) string {
	configMutex.RLock()
	hostName := latestMetric.Hostname
	configMutex.RUnlock()

	nowStr := time.Now().Format("2006-01-02 15:04:05")
	valStr := fmt.Sprintf("%.2f", value)
	thresholdStr := fmt.Sprintf("%.0f", threshold)

	res := tpl
	res = strings.ReplaceAll(res, "{{hostname}}", hostName)
	res = strings.ReplaceAll(res, "{{time}}", nowStr)
	res = strings.ReplaceAll(res, "{{type}}", typeName)
	res = strings.ReplaceAll(res, "{{value}}", valStr)
	res = strings.ReplaceAll(res, "{{threshold}}", thresholdStr)

	return res
}

func sendNotifications(msg string) {
	configMutex.RLock()
	notifyCfg := appConfig.Notify
	appName := appConfig.AppName
	configMutex.RUnlock()

	fullMsg := fmt.Sprintf("【%s】消息通知\n%s", appName, msg)
	emailSubject := fmt.Sprintf("[%s] 监控提醒", appName)

	if notifyCfg.EnableEmail {
		sendEmail(notifyCfg, emailSubject, fullMsg)
	}
	if notifyCfg.EnableDingTalk {
		sendDingTalk(notifyCfg, fullMsg)
	}
	if notifyCfg.EnableFeishu {
		sendFeishu(notifyCfg, fullMsg)
	}
}

// --- 发送通道 ---
func sendEmail(cfg NotifyConfig, subject string, body string) {
	auth := smtp.PlainAuth("", cfg.EmailUser, cfg.EmailPass, cfg.EmailHost)
	to := []string{cfg.EmailTo}
	msg := []byte("To: " + cfg.EmailTo + "\r\n" +
		"Subject: " + subject + "\r\n" +
		"Content-Type: text/plain; charset=UTF-8\r\n" +
		"\r\n" +
		body + "\r\n")

	addr := fmt.Sprintf("%s:%d", cfg.EmailHost, cfg.EmailPort)
	if err := smtp.SendMail(addr, auth, cfg.EmailUser, to, msg); err != nil {
		sendLogger.Printf("[邮件] 发送失败: %v", err)
	} else {
		sendLogger.Printf("[邮件] 发送成功 -> %s", cfg.EmailTo)
	}
}

func sendDingTalk(cfg NotifyConfig, content string) {
	payload := map[string]interface{}{"msgtype": "text", "text": map[string]string{"content": content}}
	url := fmt.Sprintf("https://oapi.dingtalk.com/robot/send?access_token=%s", cfg.DingTalkToken)
	if err := sendWebhook(url, payload); err != nil {
		sendLogger.Printf("[钉钉] 发送失败: %v", err)
	} else {
		sendLogger.Printf("[钉钉] 发送成功")
	}
}

func sendFeishu(cfg NotifyConfig, content string) {
	payload := map[string]interface{}{"msg_type": "text", "content": map[string]string{"text": content}}
	if err := sendWebhook(cfg.FeishuWebhook, payload); err != nil {
		sendLogger.Printf("[飞书] 发送失败: %v", err)
	} else {
		sendLogger.Printf("[飞书] 发送成功")
	}
}

func sendWebhook(url string, payload interface{}) error {
	data, _ := json.Marshal(payload)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

// --- 数据库与配置接口 ---

func initDB() {
	var err error
	configMutex.RLock()
	path := appConfig.DBPath
	configMutex.RUnlock()

	db, err = sql.Open("sqlite", path)
	if err != nil {
		runLogger.Fatalf("无法打开数据库: %v", err)
	}
	if err = db.Ping(); err != nil {
		runLogger.Fatalf("数据库连接失败: %v", err)
	}
	db.Exec(`CREATE TABLE IF NOT EXISTS history (timestamp INTEGER PRIMARY KEY, data TEXT);`)
}

func saveMetricToDB(m Metric) {
	// 为了节省数据库空间，历史记录中不保存 TopProcesses 和 ARP 表
	mClone := m
	// mClone.TopProcesses = nil // 注释掉此行，允许 TopProcesses 存入历史记录
	mClone.ArpTable = nil
	mClone.ListeningPorts = nil // 端口列表有时也很大，视情况保留

	data, _ := json.Marshal(mClone)
	_, err := db.Exec("INSERT INTO history (timestamp, data) VALUES (?, ?)", m.Timestamp, string(data))
	if err != nil {
		runLogger.Printf("存库失败: %v", err)
	}
}

func startRetentionPolicy() {
	for range time.Tick(1 * time.Hour) {
		configMutex.RLock()
		days := appConfig.RetentionDays
		configMutex.RUnlock()

		if days <= 0 {
			days = 30
		}

		retentionTime := time.Now().Add(time.Duration(-days) * 24 * time.Hour).Unix()
		res, err := db.Exec("DELETE FROM history WHERE timestamp < ?", retentionTime)
		if err != nil {
			runLogger.Printf("数据清理失败: %v", err)
		} else {
			count, _ := res.RowsAffected()
			if count > 0 {
				runLogger.Printf("已清理 %d 条过期数据 (保留天数: %d)", count, days)
			}
		}
	}
}

func handleConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method == http.MethodGet {
		configMutex.RLock()
		defer configMutex.RUnlock()
		json.NewEncoder(w).Encode(appConfig)
		return
	}

	if r.Method == http.MethodPost {
		var newConfig Config
		if err := json.NewDecoder(r.Body).Decode(&newConfig); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			runLogger.Printf("配置更新失败: %v", err)
			return
		}

		configMutex.Lock()
		appConfig = newConfig
		configMutex.Unlock()

		if err := saveConfigToFile(); err != nil {
			http.Error(w, "Failed to save config file", http.StatusInternalServerError)
			runLogger.Printf("保存配置文件失败: %v", err)
			return
		}

		runLogger.Println("配置已更新")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		return
	}

	http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
}

func startWebServer() {
	http.HandleFunc("/api/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		metricMutex.RLock()
		defer metricMutex.RUnlock()
		json.NewEncoder(w).Encode(latestMetric)
	})

	http.HandleFunc("/api/history", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		secondsStr := r.URL.Query().Get("seconds")
		seconds, _ := strconv.Atoi(secondsStr)
		if seconds <= 0 {
			seconds = 300
		}
		now := time.Now().Unix()
		startTime := now - int64(seconds)
		rows, err := db.Query("SELECT data FROM history WHERE timestamp >= ? ORDER BY timestamp ASC", startTime)
		if err != nil {
			runLogger.Printf("查询历史失败: %v", err)
			return
		}
		defer rows.Close()
		result := make([]Metric, 0)
		for rows.Next() {
			var dataStr string
			rows.Scan(&dataStr)
			var m Metric
			if json.Unmarshal([]byte(dataStr), &m) == nil {
				result = append(result, m)
			}
		}
		json.NewEncoder(w).Encode(result)
	})

	http.HandleFunc("/api/config", handleConfig)
	http.Handle("/", http.FileServer(http.FS(staticFiles)))

	configMutex.RLock()
	port := appConfig.ServerPort
	configMutex.RUnlock()

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
}

// --- 数据采集核心 ---

func startCollector() {
	hostInfo, _ := host.Info()
	hostStr := fmt.Sprintf("%s %s", hostInfo.Platform, hostInfo.PlatformVersion)
	pCores, _ := cpu.Counts(false)
	lCores, _ := cpu.Counts(true)
	cpuModel := getCpuModel()

	// 历史状态缓存
	lastNetStats, _ := net.IOCounters(false)
	lastDiskIO, _ := disk.IOCounters()
	lastCpuTimes, _ := cpu.Times(false)
	lastTime := time.Now()

	// 进程缓存 (PID -> *process.Process)
	// 用于保持 gopsutil 的 process 对象实例，以便准确计算 CPU 时间差
	procCache := make(map[int32]*process.Process)

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	collect := func() {
		now := time.Now()
		timeDelta := now.Sub(lastTime).Seconds()
		if timeDelta <= 0 {
			timeDelta = 1 // 避免除零
		}

		rtHostInfo, _ := host.Info()

		m := Metric{
			Timestamp:        now.Unix(),
			Hostname:         rtHostInfo.Hostname,
			OS:               hostStr,
			Kernel:           rtHostInfo.KernelVersion,
			Arch:             rtHostInfo.KernelArch,
			Uptime:           rtHostInfo.Uptime,
			BootTime:         rtHostInfo.BootTime,
			ProcessCount:     rtHostInfo.Procs,
			CpuModel:         cpuModel,
			CpuPhysicalCores: pCores,
			CpuLogicalCores:  lCores,
			TcpStateCounts:   make(map[string]int),
			ArpTable:         make([]ArpEntry, 0),
			ListeningPorts:   make([]PortInfo, 0),
			TopProcesses:     make([]ProcessInfo, 0),
		}

		// 1. CPU 使用率 (基于时间差)
		currCpuTimes, err := cpu.Times(false)
		if err == nil && len(currCpuTimes) > 0 && len(lastCpuTimes) > 0 {
			c := currCpuTimes[0]
			p := lastCpuTimes[0]
			totalDiff := c.Total() - p.Total()
			if totalDiff > 0 {
				m.CpuPercent = round((totalDiff - (c.Idle - p.Idle)) / totalDiff * 100)
				m.CpuUserPercent = round((c.User - p.User) / totalDiff * 100)
				m.CpuSystemPercent = round((c.System - p.System) / totalDiff * 100)
				m.CpuIdlePercent = round((c.Idle - p.Idle) / totalDiff * 100)
				m.CpuIowaitPercent = round((c.Iowait - p.Iowait) / totalDiff * 100)
			}
			lastCpuTimes = currCpuTimes
		}

		// 2. Load
		if l, err := load.Avg(); err == nil {
			m.Load1, m.Load5, m.Load15 = round(l.Load1), round(l.Load5), round(l.Load15)
		}

		// 3. 内存
		if v, err := mem.VirtualMemory(); err == nil {
			m.MemTotal, m.MemUsed, m.MemFree = v.Total/1024/1024, v.Used/1024/1024, v.Free/1024/1024
			m.MemBuffers, m.MemCached, m.MemPercent = v.Buffers/1024/1024, v.Cached/1024/1024, round(v.UsedPercent)
		}
		if s, err := mem.SwapMemory(); err == nil {
			m.SwapTotal, m.SwapUsed, m.SwapPercent = s.Total/1024/1024, s.Used/1024/1024, round(s.UsedPercent)
		}

		// 4. 磁盘分区
		partitions, _ := disk.Partitions(false)
		for _, part := range partitions {
			if strings.HasPrefix(part.Device, "/dev/loop") || strings.Contains(part.Fstype, "overlay") {
				continue
			}
			if u, err := disk.Usage(part.Mountpoint); err == nil {
				m.Disks = append(m.Disks, DiskInfo{
					Path: part.Mountpoint, Fstype: part.Fstype, TotalGB: round(float64(u.Total) / 1024 / 1024 / 1024), UsedGB: round(float64(u.Used) / 1024 / 1024 / 1024), UsedPercent: round(u.UsedPercent), InodesUsedPercent: round(u.InodesUsedPercent),
				})
			}
		}

		// 5. 磁盘 IO
		currDiskIO, _ := disk.IOCounters()
		var rBytes, wBytes, rCount, wCount uint64
		for _, v := range currDiskIO {
			rBytes += v.ReadBytes
			wBytes += v.WriteBytes
			rCount += v.ReadCount
			wCount += v.WriteCount
		}
		var lrBytes, lwBytes, lrCount, lwCount uint64
		for _, v := range lastDiskIO {
			lrBytes += v.ReadBytes
			lwBytes += v.WriteBytes
			lrCount += v.ReadCount
			lwCount += v.WriteCount
		}
		if timeDelta > 0 {
			m.DiskReadSpeed = round(float64(rBytes-lrBytes) / 1024 / timeDelta)
			m.DiskWriteSpeed = round(float64(wBytes-lwBytes) / 1024 / timeDelta)
			m.DiskIopsRead = round(float64(rCount-lrCount) / timeDelta)
			m.DiskIopsWrite = round(float64(wCount-lwCount) / timeDelta)
		}
		lastDiskIO = currDiskIO

		// 6. 网络 IO
		currNetStats, _ := net.IOCounters(false)
		if len(currNetStats) > 0 && len(lastNetStats) > 0 {
			c, l := currNetStats[0], lastNetStats[0]
			m.NetRxSpeed = round(float64(c.BytesRecv-l.BytesRecv) / 1024 / timeDelta)
			m.NetTxSpeed = round(float64(c.BytesSent-l.BytesSent) / 1024 / timeDelta)
			m.NetPacketsRecv = round(float64(c.PacketsRecv-l.PacketsRecv) / timeDelta)
			m.NetPacketsSent = round(float64(c.PacketsSent-l.PacketsSent) / timeDelta)
		}
		lastNetStats = currNetStats

		// --- 进程与端口信息采集 (合并逻辑) ---
		// 获取系统所有 PIDs
		pids, err := process.Pids()
		if err == nil {
			var allProcs []ProcessInfo
			// 标记当前存在的 PID
			currentPids := make(map[int32]struct{})

			for _, pid := range pids {
				currentPids[pid] = struct{}{}

				// 获取或创建缓存的 Process 对象
				proc, ok := procCache[pid]
				if !ok {
					newProc, err := process.NewProcess(pid)
					if err != nil {
						continue // 进程可能已经退出
					}
					proc = newProc
					procCache[pid] = proc
				}

				// 采集基本信息
				// 注意: CPU Percent计算需要依赖上一次调用，所以必须复用 proc 对象
				cpuPerc, _ := proc.Percent(0) // 0 表示非阻塞，返回上次调用以来的平均值
				memPerc, _ := proc.MemoryPercent()
				name, _ := proc.Name()
				username, _ := proc.Username()
				memInfo, _ := proc.MemoryInfo()
				status, _ := proc.Status() // 返回 []string，取第一个

				statusStr := ""
				if len(status) > 0 {
					statusStr = status[0]
				}

				memRSS := uint64(0)
				if memInfo != nil {
					memRSS = memInfo.RSS / 1024 / 1024 // MB
				}

				// 保存用于 Top 排序
				pInfo := ProcessInfo{
					PID:        pid,
					Name:       name,
					User:       username,
					Status:     statusStr,
					CpuPercent: round(cpuPerc),
					MemPercent: round(float64(memPerc)),
					MemRSS:     memRSS,
				}
				allProcs = append(allProcs, pInfo)
			}

			// 清理已经不存在的进程缓存
			for pid := range procCache {
				if _, ok := currentPids[pid]; !ok {
					delete(procCache, pid)
				}
			}

			// 排序并取 Top 20 (按 CPU 降序，其次按内存降序)
			sort.Slice(allProcs, func(i, j int) bool {
				if allProcs[i].CpuPercent == allProcs[j].CpuPercent {
					return allProcs[i].MemPercent > allProcs[j].MemPercent
				}
				return allProcs[i].CpuPercent > allProcs[j].CpuPercent
			})

			limit := 20
			if len(allProcs) < limit {
				limit = len(allProcs)
			}
			m.TopProcesses = allProcs[:limit]

			// --- 处理监听端口 (复用上面的 procCache) ---
			// 这样不需要在端口逻辑里重新获取 Process 对象了
			conns, err := net.Connections("inet")
			if err == nil {
				portsMap := make(map[int]PortInfo)
				for _, conn := range conns {
					m.TcpStateCounts[conn.Status]++
					if conn.Status == "LISTEN" {
						port := int(conn.Laddr.Port)
						if _, exists := portsMap[port]; !exists {
							// 尝试从缓存中找进程信息
							var pName, pUser string
							var pCpu, pMem float64
							if proc, ok := procCache[conn.Pid]; ok {
								// 数据我们在上面 Top 循环里可能已经拿到了，但那里是局部变量
								// 这里直接再读一次缓存对象的属性也很轻量
								n, _ := proc.Name()
								u, _ := proc.Username()
								c, _ := proc.Percent(0)
								mem, _ := proc.MemoryPercent()
								pName, pUser, pCpu, pMem = n, u, c, float64(mem)
							}

							if pName != "" {
								portsMap[port] = PortInfo{
									Port:        port,
									PID:         conn.Pid,
									ProcessName: pName,
									User:        pUser,
									CpuPercent:  round(pCpu),
									MemPercent:  round(pMem),
								}
							}
						}
					}
				}
				for _, pInfo := range portsMap {
					m.ListeningPorts = append(m.ListeningPorts, pInfo)
				}
				sort.Slice(m.ListeningPorts, func(i, j int) bool { return m.ListeningPorts[i].Port < m.ListeningPorts[j].Port })
			}
		}

		m.ArpTable = getArpTable()
		lastTime = now

		saveMetricToDB(m)

		metricMutex.Lock()
		latestMetric = m
		metricMutex.Unlock()

		checkAlerts(m)

		runLogger.Printf("[%s] 数据采集完成 (Top20进程已更新)...", now.Format("15:04:05"))
	}

	collect()
	for range ticker.C {
		collect()
	}
}

func getCpuModel() string {
	infos, err := cpu.Info()
	if err == nil && len(infos) > 0 {
		return infos[0].ModelName
	}
	return "Unknown"
}

func getArpTable() []ArpEntry {
	var entries []ArpEntry
	if runtime.GOOS != "linux" {
		return entries
	}
	file, err := os.Open("/proc/net/arp")
	if err != nil {
		return entries
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	if scanner.Scan() {
		_ = scanner.Text()
	}
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) >= 6 {
			entries = append(entries, ArpEntry{IPAddress: fields[0], HWAddress: fields[3], Device: fields[5]})
		}
	}
	return entries
}

func round(val float64) float64 {
	return float64(int(val*100)) / 100
}
