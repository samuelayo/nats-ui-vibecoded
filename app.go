package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/wailsapp/wails/v2/pkg/runtime"
)

type App struct {
	ctx    context.Context
	mu     sync.RWMutex
	nc     *nats.Conn
	js     jetstream.JetStream
	cancel context.CancelFunc
	store  *ProfileStore
	subs   map[string]*nats.Subscription
	conn   ConnectRequest
}

func NewApp() *App {
	return &App{subs: map[string]*nats.Subscription{}}
}

func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
	store, err := NewProfileStore()
	if err == nil {
		a.store = store
	}
}

type ConnectRequest struct {
	URL       string `json:"url"`
	Username  string `json:"username"`
	Password  string `json:"password"`
	Token     string `json:"token"`
	CredsPath string `json:"credsPath"`
}

type CLICommandRequest struct {
	Command        string `json:"command"`
	UseConnection  bool   `json:"useConnection"`
	URL            string `json:"url"`
	Username       string `json:"username"`
	Password       string `json:"password"`
	Token          string `json:"token"`
	CredsPath      string `json:"credsPath"`
	TimeoutSeconds int    `json:"timeoutSeconds"`
}

type CLICommandResult struct {
	Command        string   `json:"command"`
	Args           []string `json:"args"`
	Stdout         string   `json:"stdout"`
	Stderr         string   `json:"stderr"`
	ExitCode       int      `json:"exitCode"`
	DurationMillis int64    `json:"durationMillis"`
	StartedAt      string   `json:"startedAt"`
}

type ServerInfoData struct {
	Name             string `json:"name"`
	ServerID         string `json:"serverId"`
	Version          string `json:"version"`
	Cluster          string `json:"cluster"`
	URL              string `json:"url"`
	Address          string `json:"address"`
	ClientID         uint64 `json:"clientId"`
	MaxPayload       int64  `json:"maxPayload"`
	AuthRequired     bool   `json:"authRequired"`
	JetStreamEnabled bool   `json:"jetStreamEnabled"`
	Memory           int64  `json:"memory"`
	Storage          int64  `json:"storage"`
	Streams          int    `json:"streams"`
	Consumers        int    `json:"consumers"`
	APIRequests      int64  `json:"apiRequests"`
	APIErrors        int64  `json:"apiErrors"`
	InMsgs           uint64 `json:"inMsgs"`
	OutMsgs          uint64 `json:"outMsgs"`
	InBytes          uint64 `json:"inBytes"`
	OutBytes         uint64 `json:"outBytes"`
	Reconnects       uint64 `json:"reconnects"`
}

type StreamItem struct {
	Name          string   `json:"name"`
	Subjects      []string `json:"subjects"`
	Retention     string   `json:"retention"`
	Storage       string   `json:"storage"`
	Messages      uint64   `json:"messages"`
	Bytes         uint64   `json:"bytes"`
	FirstSeq      uint64   `json:"firstSeq"`
	LastSeq       uint64   `json:"lastSeq"`
	Deleted       uint64   `json:"deleted"`
	NumSubjects   uint64   `json:"numSubjects"`
	Consumers     int      `json:"consumers"`
	AllowDirect   bool     `json:"allowDirect"`
	AllowRollup   bool     `json:"allowRollup"`
	AllowMsgSched bool     `json:"allowMsgSched"`
	AllowAtomic   bool     `json:"allowAtomic"`
	MaxMsgsPerSub int64    `json:"maxMsgsPerSub"`
}

type StreamDetailData struct {
	ConfigJSON string         `json:"configJSON"`
	StateJSON  string         `json:"stateJSON"`
	Warnings   []string       `json:"warnings"`
	Consumers  []ConsumerItem `json:"consumers"`
}

type ConsumerItem struct {
	StreamName        string `json:"streamName"`
	Name              string `json:"name"`
	Durable           string `json:"durable"`
	FilterSubject     string `json:"filterSubject"`
	DeliverSubject    string `json:"deliverSubject"`
	DeliverGroup      string `json:"deliverGroup"`
	AckPolicy         string `json:"ackPolicy"`
	MaxAckPending     int    `json:"maxAckPending"`
	NumPending        uint64 `json:"numPending"`
	NumAckPending     int    `json:"numAckPending"`
	NumRedelivered    int    `json:"numRedelivered"`
	AckFloorStream    uint64 `json:"ackFloorStream"`
	AckFloorConsumer  uint64 `json:"ackFloorConsumer"`
	AckFloorLast      string `json:"ackFloorLast,omitempty"`
	DeliveredStream   uint64 `json:"deliveredStream"`
	DeliveredConsumer uint64 `json:"deliveredConsumer"`
	DeliveredLast     string `json:"deliveredLast,omitempty"`
	PushBound         bool   `json:"pushBound"`
}

type MessageInfo struct {
	Sequence    uint64              `json:"sequence"`
	Subject     string              `json:"subject"`
	Time        string              `json:"time"`
	Data        string              `json:"data"`
	Headers     map[string][]string `json:"headers"`
	Size        int                 `json:"size"`
	ScheduledAt string              `json:"scheduledAt"`
	Shard       string              `json:"shard"`
	Queue       string              `json:"queue"`
	Job         string              `json:"job"`
	JobID       string              `json:"jobId"`
}

type MessageFilters struct {
	SubjectContains string `json:"subjectContains"`
	PayloadContains string `json:"payloadContains"`
	HeaderKey       string `json:"headerKey"`
	HeaderValue     string `json:"headerValue"`
	Limit           int    `json:"limit"`
	MaxProbes       uint64 `json:"maxProbes"`
	Direction       string `json:"direction"`
	StartSeq        uint64 `json:"startSeq"`
}

type BucketItem struct {
	Name        string `json:"name"`
	Kind        string `json:"kind"`
	Description string `json:"description"`
	Values      uint64 `json:"values"`
	Bytes       uint64 `json:"bytes"`
	History     int64  `json:"history"`
	TTL         string `json:"ttl"`
	Storage     string `json:"storage"`
	Replicas    int    `json:"replicas"`
	Compressed  bool   `json:"compressed"`
}

type PubSubMessage struct {
	Session  string              `json:"session"`
	Received string              `json:"received"`
	Subject  string              `json:"subject"`
	Reply    string              `json:"reply"`
	Headers  map[string][]string `json:"headers"`
	Data     string              `json:"data"`
	Size     int                 `json:"size"`
}

func (a *App) Connect(req ConnectRequest) (*ServerInfoData, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.nc != nil {
		a.nc.Close()
	}
	if a.cancel != nil {
		a.cancel()
	}

	opts := []nats.Option{
		nats.Name(fmt.Sprintf("NATS-Wails-UI-%d", rand.Intn(10000))),
		nats.Timeout(10 * time.Second),
		nats.ReconnectWait(2 * time.Second),
		nats.MaxReconnects(3),
		nats.RetryOnFailedConnect(true),
	}
	if req.Username != "" {
		opts = append(opts, nats.UserInfo(req.Username, req.Password))
	}
	if req.Token != "" {
		opts = append(opts, nats.Token(req.Token))
	}
	if req.CredsPath != "" {
		opts = append(opts, nats.UserCredentials(req.CredsPath))
	}

	nc, err := nats.Connect(req.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream: %w", err)
	}

	_, cancel := context.WithCancel(context.Background())
	a.nc = nc
	a.js = js
	a.cancel = cancel
	a.conn = req
	return a.serverInfoLocked(), nil
}

func (a *App) ListProfiles() ([]ProfileView, error) {
	if a.store == nil {
		store, err := NewProfileStore()
		if err != nil {
			return nil, err
		}
		a.store = store
	}
	return a.store.ListViews()
}

func (a *App) SaveProfile(p ConnectionProfile) (int64, error) {
	if a.store == nil {
		store, err := NewProfileStore()
		if err != nil {
			return 0, err
		}
		a.store = store
	}
	return a.store.Save(p)
}

func (a *App) DeleteProfile(id int64) error {
	if a.store == nil {
		store, err := NewProfileStore()
		if err != nil {
			return err
		}
		a.store = store
	}
	return a.store.Delete(id)
}

func (a *App) ConnectProfile(id int64) (*ServerInfoData, error) {
	if a.store == nil {
		store, err := NewProfileStore()
		if err != nil {
			return nil, err
		}
		a.store = store
	}
	profile, err := a.store.Get(id)
	if err != nil {
		return nil, err
	}
	if profile == nil {
		return nil, fmt.Errorf("profile not found")
	}
	return a.Connect(ConnectRequest{
		URL:       profile.URL,
		Username:  profile.Username,
		Password:  profileSecretValue(profile.Password),
		Token:     profile.Token,
		CredsPath: profile.CredsPath,
	})
}

func (a *App) Disconnect() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cancel != nil {
		a.cancel()
	}
	if a.nc != nil {
		a.nc.Close()
	}
	a.nc = nil
	a.js = nil
	a.conn = ConnectRequest{}
}

func (a *App) Status() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.nc == nil || !a.nc.IsConnected() {
		return "Disconnected"
	}
	return "Connected to " + a.nc.ConnectedUrl()
}

func (a *App) RunNatsCLI(req CLICommandRequest) (*CLICommandResult, error) {
	args, err := splitCommandLine(req.Command)
	if err != nil {
		return nil, err
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("empty command")
	}
	if args[0] == "nats" || args[0] == "nats.exe" {
		args = args[1:]
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("provide nats arguments, for example: stream ls")
	}

	if req.UseConnection {
		conn := ConnectRequest{
			URL:       req.URL,
			Username:  req.Username,
			Password:  req.Password,
			Token:     req.Token,
			CredsPath: req.CredsPath,
		}
		a.mu.RLock()
		cached := a.conn
		a.mu.RUnlock()
		if conn.URL == "" {
			conn.URL = cached.URL
		}
		if conn.Username == "" {
			conn.Username = cached.Username
		}
		if conn.Password == "" {
			conn.Password = cached.Password
		}
		if conn.Token == "" {
			conn.Token = cached.Token
		}
		if conn.CredsPath == "" {
			conn.CredsPath = cached.CredsPath
		}

		connArgs := make([]string, 0, 8)
		if strings.TrimSpace(conn.URL) != "" {
			connArgs = append(connArgs, "--server", conn.URL)
		}
		if strings.TrimSpace(conn.Username) != "" {
			connArgs = append(connArgs, "--user", conn.Username)
		}
		if conn.Password != "" {
			connArgs = append(connArgs, "--password", conn.Password)
		}
		if conn.Token != "" {
			connArgs = append(connArgs, "--token", conn.Token)
		}
		if strings.TrimSpace(conn.CredsPath) != "" {
			connArgs = append(connArgs, "--creds", conn.CredsPath)
		}
		args = append(connArgs, args...)
	}

	timeout := time.Duration(req.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if timeout > 5*time.Minute {
		timeout = 5 * time.Minute
	}

	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "nats", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()

	maskedArgs := maskSensitiveArgs(args)
	result := &CLICommandResult{
		Command:        "nats " + strings.Join(maskedArgs, " "),
		Args:           maskedArgs,
		Stdout:         stdout.String(),
		Stderr:         stderr.String(),
		ExitCode:       0,
		DurationMillis: time.Since(started).Milliseconds(),
		StartedAt:      started.Format(time.RFC3339Nano),
	}
	if ctx.Err() == context.DeadlineExceeded {
		result.ExitCode = -1
		result.Stderr += "\ncommand timed out"
		return result, nil
	}
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			result.ExitCode = exitErr.ExitCode()
			return result, nil
		}
		return nil, fmt.Errorf("run nats: %w", err)
	}
	return result, nil
}

func maskSensitiveArgs(args []string) []string {
	masked := append([]string(nil), args...)
	for i := 0; i < len(masked); i++ {
		arg := masked[i]
		if arg == "--password" || arg == "--token" || arg == "-p" || arg == "--pass" {
			if i+1 < len(masked) {
				masked[i+1] = "***"
				i++
			}
			continue
		}
		for _, prefix := range []string{"--password=", "--token=", "--pass="} {
			if strings.HasPrefix(arg, prefix) {
				masked[i] = prefix + "***"
				break
			}
		}
	}
	return masked
}

func splitCommandLine(input string) ([]string, error) {
	var args []string
	var current strings.Builder
	var quote rune
	escaped := false

	for _, r := range input {
		switch {
		case escaped:
			current.WriteRune(r)
			escaped = false
		case r == '\\':
			escaped = true
		case quote != 0:
			if r == quote {
				quote = 0
			} else {
				current.WriteRune(r)
			}
		case r == '\'' || r == '"':
			quote = r
		case r == ' ' || r == '\t' || r == '\n' || r == '\r':
			if current.Len() > 0 {
				args = append(args, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}
	if escaped {
		current.WriteRune('\\')
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated quote")
	}
	if current.Len() > 0 {
		args = append(args, current.String())
	}
	return args, nil
}

func (a *App) GetServerInfo() (*ServerInfoData, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.nc == nil || !a.nc.IsConnected() {
		return nil, fmt.Errorf("not connected")
	}
	return a.serverInfoLocked(), nil
}

func (a *App) serverInfoLocked() *ServerInfoData {
	info := &ServerInfoData{}
	if a.nc == nil {
		return info
	}
	info.Name = a.nc.ConnectedServerName()
	info.ServerID = a.nc.ConnectedServerId()
	info.Version = a.nc.ConnectedServerVersion()
	info.Cluster = a.nc.ConnectedClusterName()
	info.URL = a.nc.ConnectedUrlRedacted()
	info.Address = a.nc.ConnectedAddr()
	info.MaxPayload = a.nc.MaxPayload()
	info.AuthRequired = a.nc.AuthRequired()
	if cid, err := a.nc.GetClientID(); err == nil {
		info.ClientID = cid
	}
	stats := a.nc.Stats()
	info.InMsgs = stats.InMsgs
	info.OutMsgs = stats.OutMsgs
	info.InBytes = stats.InBytes
	info.OutBytes = stats.OutBytes
	info.Reconnects = stats.Reconnects
	if a.js == nil {
		return info
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	accInfo, err := a.js.AccountInfo(ctx)
	if err != nil {
		return info
	}
	info.JetStreamEnabled = true
	info.Memory = int64(accInfo.Memory)
	info.Storage = int64(accInfo.Store)
	info.Streams = accInfo.Streams
	info.Consumers = accInfo.Consumers
	info.APIRequests = int64(accInfo.API.Total)
	info.APIErrors = int64(accInfo.API.Errors)
	return info
}

func (a *App) getJS() (jetstream.JetStream, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.js == nil {
		return nil, fmt.Errorf("not connected")
	}
	return a.js, nil
}

func (a *App) ListStreams() ([]StreamItem, error) {
	js, err := a.getJS()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	lister := js.ListStreams(ctx)
	var items []StreamItem
	for si := range lister.Info() {
		if si == nil {
			continue
		}
		cfg := si.Config
		st := si.State
		items = append(items, StreamItem{
			Name:          cfg.Name,
			Subjects:      cfg.Subjects,
			Retention:     retentionStr(cfg.Retention),
			Storage:       storageStr(cfg.Storage),
			Messages:      st.Msgs,
			Bytes:         st.Bytes,
			FirstSeq:      st.FirstSeq,
			LastSeq:       st.LastSeq,
			Deleted:       uint64(st.NumDeleted),
			NumSubjects:   st.NumSubjects,
			Consumers:     st.Consumers,
			AllowDirect:   cfg.AllowDirect,
			AllowRollup:   cfg.AllowRollup,
			AllowMsgSched: cfg.AllowMsgSchedules,
			AllowAtomic:   cfg.AllowAtomicPublish,
			MaxMsgsPerSub: cfg.MaxMsgsPerSubject,
		})
	}
	if err := lister.Err(); err != nil {
		return items, err
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	return items, nil
}

func (a *App) GetStreamDetail(name string) (*StreamDetailData, error) {
	js, err := a.getJS()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("stream: %w", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("stream info: %w", err)
	}
	cfg := info.Config
	st := info.State
	data := &StreamDetailData{}
	cfgB, _ := json.MarshalIndent(map[string]interface{}{
		"name": cfg.Name, "subjects": cfg.Subjects,
		"retention": retentionStr(cfg.Retention), "storage": storageStr(cfg.Storage),
		"max_msgs_per_subject": cfg.MaxMsgsPerSubject, "max_msgs": cfg.MaxMsgs,
		"max_bytes": cfg.MaxBytes, "max_age": cfg.MaxAge.String(),
		"max_msg_size": cfg.MaxMsgSize, "discard": discardStr(cfg.Discard),
		"allow_direct": cfg.AllowDirect, "allow_rollup": cfg.AllowRollup,
		"allow_msg_schedule": cfg.AllowMsgSchedules, "allow_atomic": cfg.AllowAtomicPublish,
		"num_replicas": cfg.Replicas, "sealed": cfg.Sealed,
		"deny_delete": cfg.DenyDelete, "deny_purge": cfg.DenyPurge,
		"description": cfg.Description,
	}, "", "  ")
	data.ConfigJSON = string(cfgB)
	stateB, _ := json.MarshalIndent(map[string]interface{}{
		"messages": st.Msgs, "bytes": st.Bytes,
		"first_seq": st.FirstSeq, "last_seq": st.LastSeq,
		"consumers": st.Consumers, "num_deleted": st.NumDeleted,
		"num_subjects": st.NumSubjects,
	}, "", "  ")
	data.StateJSON = string(stateB)
	if cfg.Retention == jetstream.WorkQueuePolicy && st.FirstSeq > 1 {
		data.Warnings = append(data.Warnings, fmt.Sprintf("WorkQueue stream with deleted/gapped sequences (first=%d, last=%d).", st.FirstSeq, st.LastSeq))
	}
	if cfg.MaxMsgsPerSubject > 0 {
		data.Warnings = append(data.Warnings, fmt.Sprintf("MaxMsgsPerSubject=%d set. Messages auto-remove per subject.", cfg.MaxMsgsPerSubject))
	}
	if cfg.AllowRollup {
		data.Warnings = append(data.Warnings, "Rollup headers allowed. Messages can replace earlier messages on the same subject.")
	}
	if cfg.AllowMsgSchedules {
		data.Warnings = append(data.Warnings, "Message schedules enabled. Holder messages fire later into target subjects.")
	}
	data.Consumers, _ = a.ListConsumers(name)
	return data, nil
}

func (a *App) ListConsumers(streamName string) ([]ConsumerItem, error) {
	js, err := a.getJS()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return nil, fmt.Errorf("stream: %w", err)
	}
	lister := stream.ListConsumers(ctx)
	var items []ConsumerItem
	for ci := range lister.Info() {
		if ci == nil {
			continue
		}
		cfg := ci.Config
		item := ConsumerItem{
			StreamName:        streamName,
			Name:              cfg.Name,
			Durable:           cfg.Durable,
			FilterSubject:     cfg.FilterSubject,
			DeliverSubject:    cfg.DeliverSubject,
			DeliverGroup:      cfg.DeliverGroup,
			AckPolicy:         ackPolicyStr(cfg.AckPolicy),
			MaxAckPending:     cfg.MaxAckPending,
			NumPending:        ci.NumPending,
			NumAckPending:     ci.NumAckPending,
			NumRedelivered:    ci.NumRedelivered,
			AckFloorStream:    ci.AckFloor.Stream,
			AckFloorConsumer:  ci.AckFloor.Consumer,
			AckFloorLast:      timePtrString(ci.AckFloor.Last),
			DeliveredStream:   ci.Delivered.Stream,
			DeliveredConsumer: ci.Delivered.Consumer,
			DeliveredLast:     timePtrString(ci.Delivered.Last),
			PushBound:         ci.PushBound,
		}
		if item.Durable == "" {
			item.Durable = "(ephemeral)"
		}
		items = append(items, item)
	}
	if err := lister.Err(); err != nil {
		return items, err
	}
	sort.Slice(items, func(i, j int) bool {
		return consumerScore(items[i]) > consumerScore(items[j])
	})
	return items, nil
}

func (a *App) ListAllConsumers() ([]ConsumerItem, error) {
	streams, err := a.ListStreams()
	if err != nil {
		return nil, err
	}
	var consumers []ConsumerItem
	for _, stream := range streams {
		if stream.Consumers == 0 {
			continue
		}
		cs, err := a.ListConsumers(stream.Name)
		if err == nil {
			consumers = append(consumers, cs...)
		}
	}
	sort.Slice(consumers, func(i, j int) bool {
		return consumerScore(consumers[i]) > consumerScore(consumers[j])
	})
	return consumers, nil
}

func (a *App) ConsumerCandidateMessages(c ConsumerItem, limit int) ([]MessageInfo, error) {
	if limit <= 0 {
		limit = 100
	}
	if c.DeliveredStream == 0 || c.DeliveredStream <= c.AckFloorStream {
		return nil, nil
	}
	start := c.AckFloorStream + 1
	end := c.DeliveredStream
	if end-start+1 > uint64(limit) {
		start = end - uint64(limit) + 1
	}
	msgs := make([]MessageInfo, 0, limit)
	for seq := end; seq >= start; seq-- {
		msg, err := a.GetMessage(c.StreamName, seq)
		if err == nil && msg != nil {
			msgs = append(msgs, *msg)
		}
		if seq == 0 {
			break
		}
	}
	return msgs, nil
}

func (a *App) GetMessage(streamName string, seq uint64) (*MessageInfo, error) {
	js, err := a.getJS()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return nil, fmt.Errorf("stream: %w", err)
	}
	msg, err := stream.GetMsg(ctx, seq)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, nil
	}
	return rawMsgToInfo(msg), nil
}

func (a *App) ScanMessages(streamName string, filters MessageFilters) ([]MessageInfo, error) {
	js, err := a.getJS()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return nil, fmt.Errorf("stream: %w", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("stream info: %w", err)
	}
	first, last := info.State.FirstSeq, info.State.LastSeq
	if first == 0 || last == 0 {
		return nil, nil
	}
	limit := filters.Limit
	if limit <= 0 {
		limit = 100
	}
	maxProbes := filters.MaxProbes
	if maxProbes <= 0 {
		maxProbes = 5000
	}
	backward := filters.Direction != "forward"
	cur := last
	if filters.StartSeq > 0 {
		cur = filters.StartSeq
	}
	if !backward && filters.StartSeq == 0 {
		cur = first
	}
	var out []MessageInfo
	var probes uint64
	for probes < maxProbes && len(out) < limit {
		if backward && (cur < first || cur == 0) {
			break
		}
		if !backward && cur > last {
			break
		}
		msg, err := stream.GetMsg(ctx, cur)
		probes++
		if err == nil && msg != nil {
			mi := rawMsgToInfo(msg)
			if matchesFilters(mi, filters) {
				out = append(out, *mi)
			}
		}
		if backward {
			cur--
		} else {
			cur++
		}
	}
	return out, nil
}

func (a *App) ScanMessagesStream(streamName string, filters MessageFilters, session string) error {
	js, err := a.getJS()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return fmt.Errorf("stream: %w", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return fmt.Errorf("stream info: %w", err)
	}
	first, last := info.State.FirstSeq, info.State.LastSeq
	if first == 0 || last == 0 {
		runtime.EventsEmit(a.ctx, "scan:progress", map[string]interface{}{"session": session, "done": true, "matched": 0, "scanned": 0, "current": 0})
		return nil
	}
	limit := filters.Limit
	if limit <= 0 {
		limit = 100
	}
	maxProbes := filters.MaxProbes
	if maxProbes <= 0 {
		maxProbes = 5000
	}
	backward := filters.Direction != "forward"
	cur := last
	if filters.StartSeq > 0 {
		cur = filters.StartSeq
	}
	if !backward && filters.StartSeq == 0 {
		cur = first
	}
	matched := 0
	var probes uint64
	for probes < maxProbes && matched < limit {
		if backward && (cur < first || cur == 0) {
			break
		}
		if !backward && cur > last {
			break
		}
		msg, err := stream.GetMsg(ctx, cur)
		probes++
		if err == nil && msg != nil {
			mi := rawMsgToInfo(msg)
			if matchesFilters(mi, filters) {
				matched++
				runtime.EventsEmit(a.ctx, "scan:message", map[string]interface{}{"session": session, "message": mi})
			}
		}
		if probes%25 == 0 || matched == limit {
			runtime.EventsEmit(a.ctx, "scan:progress", map[string]interface{}{"session": session, "done": false, "matched": matched, "scanned": probes, "current": cur})
		}
		if backward {
			cur--
		} else {
			cur++
		}
	}
	runtime.EventsEmit(a.ctx, "scan:progress", map[string]interface{}{"session": session, "done": true, "matched": matched, "scanned": probes, "current": cur})
	return nil
}

func (a *App) RepublishMessage(subject string, data string, headers map[string][]string) error {
	a.mu.RLock()
	nc := a.nc
	a.mu.RUnlock()
	if nc == nil || !nc.IsConnected() {
		return fmt.Errorf("not connected")
	}
	msg := nats.NewMsg(subject)
	msg.Data = []byte(data)
	for k, vals := range headers {
		for _, v := range vals {
			msg.Header.Add(k, v)
		}
	}
	return nc.PublishMsg(msg)
}

func (a *App) CorePublish(subject string, data string, headers map[string]string) error {
	a.mu.RLock()
	nc := a.nc
	a.mu.RUnlock()
	if nc == nil || !nc.IsConnected() {
		return fmt.Errorf("not connected")
	}
	msg := nats.NewMsg(subject)
	msg.Data = []byte(data)
	for k, v := range headers {
		if strings.TrimSpace(k) != "" {
			msg.Header.Set(k, v)
		}
	}
	return nc.PublishMsg(msg)
}

func (a *App) SubscribeCore(subject, queue, session string) error {
	a.mu.RLock()
	nc := a.nc
	a.mu.RUnlock()
	if nc == nil || !nc.IsConnected() {
		return fmt.Errorf("not connected")
	}
	handler := func(msg *nats.Msg) {
		runtime.EventsEmit(a.ctx, "pubsub:message", map[string]interface{}{
			"session": session,
			"message": PubSubMessage{
				Session: session, Received: time.Now().Format(time.RFC3339Nano), Subject: msg.Subject, Reply: msg.Reply,
				Headers: map[string][]string(msg.Header), Data: string(msg.Data), Size: len(msg.Data),
			},
		})
	}
	var sub *nats.Subscription
	var err error
	if strings.TrimSpace(queue) != "" {
		sub, err = nc.QueueSubscribe(subject, queue, handler)
	} else {
		sub, err = nc.Subscribe(subject, handler)
	}
	if err != nil {
		return err
	}
	a.mu.Lock()
	if old := a.subs[session]; old != nil {
		_ = old.Unsubscribe()
	}
	a.subs[session] = sub
	a.mu.Unlock()
	return nil
}

func (a *App) UnsubscribeCore(session string) error {
	a.mu.Lock()
	sub := a.subs[session]
	delete(a.subs, session)
	a.mu.Unlock()
	if sub != nil {
		return sub.Unsubscribe()
	}
	return nil
}

func (a *App) ListKeyValueBuckets() ([]BucketItem, error) {
	js, err := a.getJS()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	lister := js.KeyValueStores(ctx)
	var items []BucketItem
	for st := range lister.Status() {
		if st == nil {
			continue
		}
		items = append(items, BucketItem{Name: st.Bucket(), Kind: "Key-Value", Values: st.Values(), Bytes: st.Bytes(), History: st.History(), TTL: st.TTL().String(), Storage: st.BackingStore(), Compressed: st.IsCompressed()})
	}
	return items, lister.Error()
}

func (a *App) ListObjectStores() ([]BucketItem, error) {
	js, err := a.getJS()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	lister := js.ObjectStores(ctx)
	var items []BucketItem
	for st := range lister.Status() {
		if st == nil {
			continue
		}
		items = append(items, BucketItem{Name: st.Bucket(), Kind: "Object Store", Description: st.Description(), Bytes: st.Size(), TTL: st.TTL().String(), Storage: storageStr(st.Storage()), Replicas: st.Replicas(), Compressed: st.IsCompressed()})
	}
	return items, lister.Error()
}

func rawMsgToInfo(msg *jetstream.RawStreamMsg) *MessageInfo {
	headers := map[string][]string(nats.Header(msg.Header))
	data := string(msg.Data)
	parts := parseSchedulerSubject(msg.Subject)
	info := &MessageInfo{
		Sequence: msg.Sequence,
		Subject:  msg.Subject,
		Time:     msg.Time.Format(time.RFC3339Nano),
		Data:     data,
		Headers:  headers,
		Size:     len(msg.Data),
		Shard:    parts.Shard,
		Queue:    parts.Queue,
		Job:      parts.Job,
		JobID:    parts.JobID,
	}
	info.ScheduledAt = scheduledAtDisplay(msg.Data, headers)
	return info
}

type schedulerSubjectParts struct {
	Shard string
	Queue string
	Job   string
	JobID string
}

func parseSchedulerSubject(subject string) schedulerSubjectParts {
	parts := strings.Split(subject, ".")
	if len(parts) >= 5 && parts[0] == "sched" {
		jobID := parts[len(parts)-1]
		if jobID == "due" && len(parts) >= 6 {
			jobID = parts[len(parts)-2]
		}
		return schedulerSubjectParts{Shard: parts[1], Queue: parts[2], Job: parts[3], JobID: jobID}
	}
	return schedulerSubjectParts{Shard: "-", Queue: "-", Job: "-", JobID: subject}
}

func scheduledAtDisplay(data []byte, headers map[string][]string) string {
	var payload struct {
		ScheduledAt string `json:"scheduled_at"`
	}
	if err := json.Unmarshal(data, &payload); err == nil && payload.ScheduledAt != "" {
		return payload.ScheduledAt
	}
	if vals, ok := headers["Nats-Schedule"]; ok && len(vals) > 0 {
		return strings.TrimPrefix(vals[0], "@at ")
	}
	return ""
}

func matchesFilters(mi *MessageInfo, f MessageFilters) bool {
	if f.SubjectContains != "" && !strings.Contains(strings.ToLower(mi.Subject), strings.ToLower(f.SubjectContains)) {
		return false
	}
	if f.PayloadContains != "" && !strings.Contains(strings.ToLower(mi.Data), strings.ToLower(f.PayloadContains)) {
		return false
	}
	if f.HeaderKey != "" {
		vals, ok := mi.Headers[f.HeaderKey]
		if !ok {
			return false
		}
		if f.HeaderValue != "" {
			for _, v := range vals {
				if strings.Contains(strings.ToLower(v), strings.ToLower(f.HeaderValue)) {
					return true
				}
			}
			return false
		}
	}
	return true
}

func retentionStr(r jetstream.RetentionPolicy) string {
	switch r {
	case jetstream.LimitsPolicy:
		return "Limits"
	case jetstream.InterestPolicy:
		return "Interest"
	case jetstream.WorkQueuePolicy:
		return "WorkQueue"
	default:
		return "Unknown"
	}
}

func storageStr(s jetstream.StorageType) string {
	switch s {
	case jetstream.FileStorage:
		return "File"
	case jetstream.MemoryStorage:
		return "Memory"
	default:
		return "Unknown"
	}
}

func ackPolicyStr(a jetstream.AckPolicy) string {
	switch a {
	case jetstream.AckExplicitPolicy:
		return "Explicit"
	case jetstream.AckAllPolicy:
		return "All"
	case jetstream.AckNonePolicy:
		return "None"
	default:
		return "Unknown"
	}
}

func discardStr(d jetstream.DiscardPolicy) string {
	switch d {
	case jetstream.DiscardOld:
		return "Old"
	case jetstream.DiscardNew:
		return "New"
	default:
		return "Unknown"
	}
}

func consumerScore(c ConsumerItem) int {
	return c.NumRedelivered*1000 + c.NumAckPending*100 + int(c.NumPending/100)
}

func prettyJSON(s string) string {
	var out bytes.Buffer
	if err := json.Indent(&out, []byte(s), "", "  "); err == nil {
		return out.String()
	}
	return s
}

func timePtrString(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}

var _ = errors.Is
