package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"
	"github.com/joho/godotenv"
	"golang.org/x/net/proxy"
)

const (
	DEFAULT_PROXY     = "socks5://localhost:7890"
	SESSION_FILE      = ".td.session"
	DEFAULT_OUTPUT    = "./messages.jsonl"
	DEFAULT_PAGE_SIZE = 100
	MAX_PAGE_SIZE     = 100
)

var stdinReader = bufio.NewReader(os.Stdin)

func readLine() (string, error) {
	val, err := stdinReader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(val), nil
}

// localZone 从 TZ 环境变量加载时区，无法解析则退回香港时区 (UTC+8)
func localZone() *time.Location {
	if tz := os.Getenv("TZ"); tz != "" {
		if loc, err := time.LoadLocation(tz); err == nil {
			return loc
		}
	}
	return time.FixedZone("HKT", 8*3600)
}

func hktNow() string {
	return time.Now().In(localZone()).Format(time.RFC3339)
}

func LogInfo(format string, v ...any) {
	fmt.Printf("[%s] [INFO] %s\n", hktNow(), fmt.Sprintf(format, v...))
}

func LogError(err error, format string, v ...any) {
	fmt.Printf("[%s] [ERROR] %s: %v\n", hktNow(), fmt.Sprintf(format, v...), err)
}

// resolveProxyURL 按优先级确定代理地址：
// CLI flag > ALL_PROXY 环境变量 > all_proxy 环境变量 > 内置默认值
func resolveProxyURL(flagVal string) string {
	if flagVal != "" {
		return flagVal
	}
	for _, key := range []string{"ALL_PROXY", "all_proxy"} {
		if v := os.Getenv(key); v != "" {
			return v
		}
	}
	return DEFAULT_PROXY
}

// newProxyDialer 解析 socks5://[user:pass@]host:port 格式并创建 SOCKS5 拨号器
func newProxyDialer(rawURL string) (proxy.ContextDialer, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("代理地址解析失败 (%s): %w", rawURL, err)
	}

	var auth *proxy.Auth
	if u.User != nil {
		auth = &proxy.Auth{User: u.User.Username()}
		if pass, ok := u.User.Password(); ok {
			auth.Password = pass
		}
	}

	dialer, err := proxy.SOCKS5("tcp", u.Host, auth, proxy.Direct)
	if err != nil {
		return nil, fmt.Errorf("初始化 SOCKS5 代理失败: %w", err)
	}
	return dialer.(proxy.ContextDialer), nil
}

// TerminalAuth 终端交互式鉴权实现
type TerminalAuth struct {
	phone string
}

func (a TerminalAuth) Phone(_ context.Context) (string, error) {
	return a.phone, nil
}

func (a TerminalAuth) Password(_ context.Context) (string, error) {
	fmt.Print("请输入两步验证密码: ")
	return readLine()
}

func (a TerminalAuth) AcceptTermsOfService(_ context.Context, tos tg.HelpTermsOfService) error {
	return &auth.SignUpRequired{TermsOfService: tos}
}

func (a TerminalAuth) SignUp(_ context.Context) (auth.UserInfo, error) {
	return auth.UserInfo{}, errors.New("不支持在此程序注册新账号，请先在手机客户端注册")
}

func (a TerminalAuth) Code(_ context.Context, _ *tg.AuthSentCode) (string, error) {
	fmt.Print("请输入收到的验证码: ")
	return readLine()
}

// AppConfig 从环境变量加载的配置
type AppConfig struct {
	AppID            int
	AppHash          string
	Phone            string
	ChannelUsernames []string
}

// loadConfig 读取并校验必需的环境变量。
// TG_CHANNEL_USERNAME 可留空，由调用方通过 -channels flag 覆盖。
func loadConfig() (*AppConfig, error) {
	envFile := os.Getenv("ENV_FILE")
	if envFile == "" {
		envFile = ".env"
	}
	log.Printf("正在加载环境变量文件: %s", envFile)
	_ = godotenv.Load(envFile)

	appIDStr := os.Getenv("TG_API_ID")
	appHash := os.Getenv("TG_API_HASH")
	phone := os.Getenv("TG_PHONE")

	if appIDStr == "" || appHash == "" || phone == "" {
		return nil, errors.New(
			"缺少必须的环境变量: TG_API_ID, TG_API_HASH, TG_PHONE",
		)
	}

	appID, err := strconv.Atoi(appIDStr)
	if err != nil {
		return nil, fmt.Errorf("TG_API_ID 解析失败: %w", err)
	}

	return &AppConfig{
		AppID:            appID,
		AppHash:          appHash,
		Phone:            phone,
		ChannelUsernames: parseUsernames(os.Getenv("TG_CHANNEL_USERNAME")),
	}, nil
}

// parseUsernames 将逗号分隔的频道名字符串解析为切片
func parseUsernames(raw string) []string {
	var result []string
	for _, u := range strings.Split(raw, ",") {
		if u = strings.TrimSpace(u); u != "" {
			result = append(result, u)
		}
	}
	return result
}

// EntityInfo 消息文本中的实体（链接、提及、话题标签、代码等）
type EntityInfo struct {
	Type string `json:"type"`
	Text string `json:"text"`
	URL  string `json:"url,omitempty"`
}

// MediaInfo 消息附件的关键元数据
type MediaInfo struct {
	Type         string   `json:"type"`
	URL          string   `json:"url,omitempty"`
	Title        string   `json:"title,omitempty"`
	Description  string   `json:"description,omitempty"`
	MimeType     string   `json:"mime_type,omitempty"`
	FileName     string   `json:"file_name,omitempty"`
	PollQuestion string   `json:"poll_question,omitempty"`
	PollOptions  []string `json:"poll_options,omitempty"`
}

// MessageRecord 单条消息的完整结构化记录
type MessageRecord struct {
	ID            int          `json:"id"`
	Timestamp     string       `json:"timestamp"`
	EditedAt      string       `json:"edited_at,omitempty"`
	Text          string       `json:"text"`
	ChannelID     int64        `json:"channel_id"`
	Channel       string       `json:"channel"`
	Views         int          `json:"views,omitempty"`
	Forwards      int          `json:"forwards,omitempty"`
	Replies       int          `json:"replies,omitempty"`
	Entities      []EntityInfo `json:"entities,omitempty"`
	Media         *MediaInfo   `json:"media,omitempty"`
	ForwardedFrom string       `json:"forwarded_from,omitempty"`
	PostAuthor    string       `json:"post_author,omitempty"`
	ReplyToID     int          `json:"reply_to_id,omitempty"`
}

// MessageWriter 线程安全的 JSONL 消息写入器
type MessageWriter struct {
	mu   sync.Mutex
	file *os.File
}

func NewMessageWriter(path string) (*MessageWriter, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("打开输出文件失败 (%s): %w", path, err)
	}
	return &MessageWriter{file: file}, nil
}

func (w *MessageWriter) Write(record MessageRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err = fmt.Fprintf(w.file, "%s\n", data)
	return err
}

func (w *MessageWriter) Close() error {
	return w.file.Close()
}

// loadExistingIDs 读取 JSONL 文件中已有的消息 ID 集合。
// 指定 channelID > 0 时只收录该频道的 ID（用于多频道共享同一文件的场景）。
func loadExistingIDs(path string, channelID int64) (map[int]bool, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[int]bool), nil
		}
		return nil, fmt.Errorf("读取已有消息文件失败: %w", err)
	}
	defer file.Close()

	existingIDs := make(map[int]bool)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		var record MessageRecord
		if jsonErr := json.Unmarshal(scanner.Bytes(), &record); jsonErr != nil {
			continue
		}
		if channelID <= 0 || record.ChannelID == channelID {
			existingIDs[record.ID] = true
		}
	}
	return existingIDs, scanner.Err()
}

// ChannelCtx 单个频道的运行时状态
type ChannelCtx struct {
	Channel     *tg.Channel
	Peer        *tg.InputPeerChannel
	Writer      *MessageWriter
	ExistingIDs map[int]bool
}

// channelFileKey 返回频道文件名 key：优先使用 username，否则退回数字 ID
func channelFileKey(channel *tg.Channel) string {
	if channel.Username != "" {
		return channel.Username
	}
	return strconv.FormatInt(int64(channel.ID), 10)
}

// openChannelWriter 根据是否指定输出目录决定写入路径。
// 有 outputDir 时每个频道单独文件（{outputDir}/{fileKey}.jsonl），否则共用 outputPath。
func openChannelWriter(outputPath, outputDir, fileKey string) (*MessageWriter, error) {
	path := outputPath
	if outputDir != "" {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			return nil, fmt.Errorf("创建输出目录失败 (%s): %w", outputDir, err)
		}
		path = filepath.Join(outputDir, fileKey+".jsonl")
	}
	return NewMessageWriter(path)
}

// resolveExistingIDsPath 根据输出模式确定读取已有 ID 的文件路径和过滤用的频道 ID
func resolveExistingIDsPath(outputPath, outputDir, fileKey string, channelID int64) (string, int64) {
	if outputDir != "" {
		// 每个频道独立文件，无需按 channelID 过滤
		return filepath.Join(outputDir, fileKey+".jsonl"), 0
	}
	// 共享文件，按 channelID 过滤
	return outputPath, channelID
}

func extractEntities(text string, entities []tg.MessageEntityClass) []EntityInfo {
	runes := []rune(text)
	total := len(runes)
	var result []EntityInfo

	for _, ent := range entities {
		var entityType, extraURL string
		var offset, length int

		switch e := ent.(type) {
		case *tg.MessageEntityURL:
			entityType, offset, length = "url", e.Offset, e.Length
		case *tg.MessageEntityTextURL:
			entityType, offset, length, extraURL = "text_url", e.Offset, e.Length, e.URL
		case *tg.MessageEntityMention:
			entityType, offset, length = "mention", e.Offset, e.Length
		case *tg.MessageEntityMentionName:
			entityType, offset, length = "mention", e.Offset, e.Length
		case *tg.MessageEntityHashtag:
			entityType, offset, length = "hashtag", e.Offset, e.Length
		case *tg.MessageEntityCashtag:
			entityType, offset, length = "cashtag", e.Offset, e.Length
		case *tg.MessageEntityCode:
			entityType, offset, length = "code", e.Offset, e.Length
		case *tg.MessageEntityPre:
			entityType, offset, length = "pre", e.Offset, e.Length
		case *tg.MessageEntityEmail:
			entityType, offset, length = "email", e.Offset, e.Length
		case *tg.MessageEntityPhone:
			entityType, offset, length = "phone", e.Offset, e.Length
		case *tg.MessageEntityBotCommand:
			entityType, offset, length = "bot_command", e.Offset, e.Length
		default:
			continue
		}

		if offset < 0 || length <= 0 || offset+length > total {
			continue
		}
		result = append(result, EntityInfo{
			Type: entityType,
			Text: string(runes[offset : offset+length]),
			URL:  extraURL,
		})
	}
	return result
}

func extractMedia(media tg.MessageMediaClass) *MediaInfo {
	if media == nil {
		return nil
	}
	switch m := media.(type) {
	case *tg.MessageMediaPhoto:
		return &MediaInfo{Type: "photo"}
	case *tg.MessageMediaDocument:
		info := &MediaInfo{Type: "document"}
		if doc, ok := m.Document.(*tg.Document); ok {
			info.MimeType = doc.MimeType
			for _, attr := range doc.Attributes {
				switch a := attr.(type) {
				case *tg.DocumentAttributeFilename:
					info.FileName = a.FileName
				case *tg.DocumentAttributeVideo:
					if a.RoundMessage {
						info.Type = "round_video"
					} else {
						info.Type = "video"
					}
				case *tg.DocumentAttributeAudio:
					if a.Voice {
						info.Type = "voice"
					} else {
						info.Type = "audio"
					}
				case *tg.DocumentAttributeSticker:
					info.Type = "sticker"
					info.Title = a.Alt
				case *tg.DocumentAttributeAnimated:
					info.Type = "gif"
				}
			}
		}
		return info
	case *tg.MessageMediaWebPage:
		info := &MediaInfo{Type: "webpage"}
		if wp, ok := m.Webpage.(*tg.WebPage); ok {
			info.URL = wp.URL
			info.Title = wp.Title
			info.Description = wp.Description
		}
		return info
	case *tg.MessageMediaPoll:
		info := &MediaInfo{Type: "poll", PollQuestion: m.Poll.Question.Text}
		for _, ans := range m.Poll.Answers {
			info.PollOptions = append(info.PollOptions, ans.Text.Text)
		}
		return info
	case *tg.MessageMediaGeo:
		return &MediaInfo{Type: "geo"}
	case *tg.MessageMediaContact:
		return &MediaInfo{Type: "contact", Title: m.FirstName + " " + m.LastName}
	case *tg.MessageMediaDice:
		return &MediaInfo{Type: "dice", Title: m.Emoticon}
	default:
		return nil
	}
}

func buildRecord(msg *tg.Message, channelID int64, channelTitle string) MessageRecord {
	loc := localZone()
	msgTime := time.Unix(int64(msg.Date), 0).In(loc)

	record := MessageRecord{
		ID:         msg.ID,
		Timestamp:  msgTime.Format(time.RFC3339),
		Text:       msg.Message,
		ChannelID:  channelID,
		Channel:    channelTitle,
		Views:      msg.Views,
		Forwards:   msg.Forwards,
		PostAuthor: msg.PostAuthor,
		Entities:   extractEntities(msg.Message, msg.Entities),
		Media:      extractMedia(msg.Media),
	}

	if msg.EditDate != 0 {
		record.EditedAt = time.Unix(int64(msg.EditDate), 0).In(loc).Format(time.RFC3339)
	}
	if replies, ok := msg.GetReplies(); ok {
		record.Replies = replies.Replies
	}
	if fwdFrom, ok := msg.GetFwdFrom(); ok {
		if fwdFrom.FromName != "" {
			record.ForwardedFrom = fwdFrom.FromName
		} else {
			record.ForwardedFrom = "forwarded"
		}
	}
	if msg.ReplyTo != nil {
		if replyHeader, ok := msg.ReplyTo.(*tg.MessageReplyHeader); ok {
			record.ReplyToID = replyHeader.ReplyToMsgID
		}
	}

	return record
}

func printRecord(record MessageRecord, label string) {
	text := record.Text
	runes := []rune(text)
	if len(runes) > 120 {
		text = string(runes[:120]) + "..."
	}
	text = strings.ReplaceAll(text, "\n", " ")
	fmt.Printf("  [%s] [%s] [%s] #%d: %s\n", label, record.Channel, record.Timestamp, record.ID, text)
}

func extractMessages(result tg.MessagesMessagesClass) []tg.MessageClass {
	switch r := result.(type) {
	case *tg.MessagesMessages:
		return r.Messages
	case *tg.MessagesMessagesSlice:
		return r.Messages
	case *tg.MessagesChannelMessages:
		return r.Messages
	default:
		return nil
	}
}

func oldestMessageID(messages []tg.MessageClass) (int, bool) {
	for i := len(messages) - 1; i >= 0; i-- {
		if msg, ok := messages[i].(*tg.Message); ok {
			return msg.ID, true
		}
	}
	return 0, false
}

func joinChannelIfNeeded(ctx context.Context, api *tg.Client, channel *tg.Channel) error {
	if !channel.Left {
		LogInfo("已是频道成员: %s", channel.Title)
		return nil
	}

	inputChannel := &tg.InputChannel{
		ChannelID:  channel.ID,
		AccessHash: channel.AccessHash,
	}

	if _, err := api.ChannelsJoinChannel(ctx, inputChannel); err != nil {
		if strings.Contains(err.Error(), "ALREADY_PARTICIPANT") {
			return nil
		}
		return fmt.Errorf("加入频道失败: %w", err)
	}

	LogInfo("成功加入频道: %s", channel.Title)
	return nil
}

// fetchHistoryInteractive 交互式分页拉取历史消息，收集完毕后按时间正序写入文件。
//
// autoFetch=true 时跳过"是否拉取历史消息"确认；autoStopOnDup=true 时遇重复自动停止。
// 若 existingIDs 为空（首次运行），则直接开始拉取，不询问。
func fetchHistoryInteractive(
	ctx context.Context,
	api *tg.Client,
	chCtx *ChannelCtx,
	pageSize int,
	autoFetch bool,
	autoStopOnDup bool,
) error {
	channel := chCtx.Channel
	channelID := int64(channel.ID)
	channelTitle := channel.Title

	isFreshRun := len(chCtx.ExistingIDs) == 0

	// 确认是否拉取历史消息：首次运行直接开始，有参数或默认 Y 也继续
	if !isFreshRun && !autoFetch {
		fmt.Printf("\n[%s] 是否拉取历史消息? [Y/n]: ", channelTitle)
		answer, err := readLine()
		if err != nil {
			return err
		}
		if strings.ToLower(answer) == "n" {
			LogInfo("[%s] 跳过历史消息拉取", channelTitle)
			return nil
		}
	}

	var collected []MessageRecord
	offsetID := 0
	pageNum := 0
	autoAll := false

	for {
		result, err := api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:     chCtx.Peer,
			Limit:    pageSize,
			OffsetID: offsetID,
		})
		if err != nil {
			return fmt.Errorf("[%s] 拉取历史消息失败: %w", channelTitle, err)
		}

		messages := extractMessages(result)
		if len(messages) == 0 {
			break
		}

		pageNum++
		fmt.Printf("\n[%s] --- 第 %d 页（本页 %d 条）---\n", channelTitle, pageNum, len(messages))

		stopHere := false
		for _, msgClass := range messages {
			msg, ok := msgClass.(*tg.Message)
			if !ok {
				continue
			}

			if chCtx.ExistingIDs[msg.ID] {
				record := buildRecord(msg, channelID, channelTitle)
				if autoStopOnDup {
					LogInfo("[%s] 遇到已存在的消息 #%d，自动停止", channelTitle, msg.ID)
					stopHere = true
					break
				}
				fmt.Printf("\n发现已存在的消息 #%d（%s），停止拉取? [Y/n]: ", msg.ID, record.Timestamp)
				input, err := readLine()
				if err != nil {
					return err
				}
				if strings.ToLower(input) != "n" {
					LogInfo("[%s] 遇到已有消息，停止拉取历史", channelTitle)
					stopHere = true
					break
				}
				continue
			}

			record := buildRecord(msg, channelID, channelTitle)
			printRecord(record, "历史")
			collected = append(collected, record)
		}

		if stopHere {
			goto write
		}

		if minID, ok := oldestMessageID(messages); ok {
			offsetID = minID
		}

		if len(messages) < pageSize {
			break
		}

		if autoAll {
			continue
		}

		fmt.Printf("[%s] [Enter] 继续  [a] 全部拉取  [q] 停止: ", channelTitle)
		input, err := readLine()
		if err != nil {
			return err
		}
		switch input {
		case "q":
			LogInfo("[%s] 已停止翻页，将写入已收集的 %d 条消息", channelTitle, len(collected))
			goto write
		case "a":
			autoAll = true
			LogInfo("[%s] 切换为自动拉取全部历史...", channelTitle)
		}
	}

	LogInfo("[%s] 已到达历史记录最顶部，共收集 %d 条消息", channelTitle, len(collected))

write:
	for i := len(collected) - 1; i >= 0; i-- {
		if writeErr := chCtx.Writer.Write(collected[i]); writeErr != nil {
			LogError(writeErr, "[%s] 写入消息失败", channelTitle)
		}
	}
	if len(collected) > 0 {
		LogInfo("[%s] 历史消息已按时间正序写入，共 %d 条", channelTitle, len(collected))
	}

	return nil
}

func main() {
	proxyFlag := flag.String("proxy", "", "SOCKS5 代理地址，格式 socks5://[user:pass@]host:port\n留空则依次读取 ALL_PROXY 环境变量，最终兜底 "+DEFAULT_PROXY)
	outputPath := flag.String("output", DEFAULT_OUTPUT, "消息输出文件（JSONL 格式），多频道共用\n被 -output-dir 覆盖时忽略")
	outputDir := flag.String("output-dir", "", "消息输出目录，优先级高于 -output\n每个频道写入 {channel username}.jsonl")
	channelsFlag := flag.String("channels", "", "要监听的频道用户名，逗号分隔\n覆盖 TG_CHANNEL_USERNAME 环境变量")
	fetchHistory := flag.Bool("history", false, "跳过「是否拉取历史消息」的交互确认，直接开始拉取")
	pageSize := flag.Int("page-size", DEFAULT_PAGE_SIZE, "每次拉取历史消息的条数，默认 100，最大 "+strconv.Itoa(MAX_PAGE_SIZE))
	stopOnDup := flag.Bool("auto-stop", false, "翻页时遇到已存在的消息，自动停止而不弹确认提示。\n否则用于回填历史消息空缺。")
	flag.Parse()

	if *pageSize > MAX_PAGE_SIZE {
		LogInfo("-page-size 超过 API 上限，已截断为 %d", MAX_PAGE_SIZE)
		*pageSize = MAX_PAGE_SIZE
	}
	if *pageSize < 1 {
		*pageSize = 1
	}

	config, err := loadConfig()
	if err != nil {
		LogError(err, "配置读取失败")
		return
	}

	// -channels flag 优先级高于 TG_CHANNEL_USERNAME 环境变量
	if *channelsFlag != "" {
		config.ChannelUsernames = parseUsernames(*channelsFlag)
	}
	if len(config.ChannelUsernames) == 0 {
		LogError(errors.New("未指定任何频道"), "请通过 -channels flag 或 TG_CHANNEL_USERNAME 环境变量提供频道名")
		return
	}

	// Telegram 使用原始 TCP，需通过 SOCKS5 代理
	proxyURL := resolveProxyURL(*proxyFlag)
	LogInfo("使用代理: %s", proxyURL)
	contextDialer, err := newProxyDialer(proxyURL)
	if err != nil {
		LogError(err, "初始化代理失败")
		return
	}

	// 历史写入完成前缓冲实时消息，防止文件乱序
	var (
		rtMu          sync.Mutex
		rtBuf         []MessageRecord
		rtHistoryDone bool
	)

	// channelCtxMap 在 Run 内部填充后供 dispatcher 使用
	channelCtxMap := make(map[int64]*ChannelCtx)

	dispatcher := tg.NewUpdateDispatcher()
	opts := telegram.Options{
		SessionStorage: &session.FileStorage{Path: SESSION_FILE},
		Resolver:       dcs.Plain(dcs.PlainOptions{Dial: contextDialer.DialContext}),
		UpdateHandler:  dispatcher,
	}

	client := telegram.NewClient(config.AppID, config.AppHash, opts)
	flow := auth.NewFlow(TerminalAuth{phone: config.Phone}, auth.SendCodeOptions{})

	dispatcher.OnNewChannelMessage(func(ctx context.Context, e tg.Entities, update *tg.UpdateNewChannelMessage) error {
		msg, ok := update.Message.(*tg.Message)
		if !ok || msg.Message == "" {
			return nil
		}

		peerChannel, ok := msg.PeerID.(*tg.PeerChannel)
		if !ok {
			return nil
		}

		chCtx, exists := channelCtxMap[peerChannel.ChannelID]
		if !exists {
			return nil
		}

		record := buildRecord(msg, int64(chCtx.Channel.ID), chCtx.Channel.Title)
		printRecord(record, "实时")

		rtMu.Lock()
		if !rtHistoryDone {
			rtBuf = append(rtBuf, record)
			rtMu.Unlock()
			return nil
		}
		rtMu.Unlock()

		if writeErr := chCtx.Writer.Write(record); writeErr != nil {
			LogError(writeErr, "[%s] 写入实时消息失败", chCtx.Channel.Title)
		}
		return nil
	})

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	LogInfo("正在启动 Telegram 客户端...")

	err = client.Run(ctx, func(ctx context.Context) error {
		if err := client.Auth().IfNecessary(ctx, flow); err != nil {
			return fmt.Errorf("鉴权失败: %w", err)
		}
		LogInfo("登录成功，账号: %s", config.Phone)

		api := client.API()

		// 逐一解析、加入频道并初始化 ChannelCtx
		for _, username := range config.ChannelUsernames {
			resolved, err := api.ContactsResolveUsername(ctx, &tg.ContactsResolveUsernameRequest{
				Username: username,
			})
			if err != nil {
				return fmt.Errorf("解析频道失败 (%s): %w", username, err)
			}

			var channel *tg.Channel
			for _, chat := range resolved.Chats {
				if ch, ok := chat.(*tg.Channel); ok {
					channel = ch
					break
				}
			}
			if channel == nil {
				return fmt.Errorf("未找到频道: %s", username)
			}

			LogInfo("成功解析频道: %s (ID: %d)", channel.Title, channel.ID)

			if err := joinChannelIfNeeded(ctx, api, channel); err != nil {
				return err
			}

			fileKey := channelFileKey(channel)
			idPath, filterID := resolveExistingIDsPath(*outputPath, *outputDir, fileKey, int64(channel.ID))
			existingIDs, err := loadExistingIDs(idPath, filterID)
			if err != nil {
				return err
			}
			if len(existingIDs) > 0 {
				LogInfo("[%s] 检测到已有 %d 条消息记录", channel.Title, len(existingIDs))
			}

			writer, err := openChannelWriter(*outputPath, *outputDir, fileKey)
			if err != nil {
				return fmt.Errorf("[%s] 初始化输出文件失败: %w", channel.Title, err)
			}
			defer writer.Close()

			channelCtxMap[int64(channel.ID)] = &ChannelCtx{
				Channel: channel,
				Peer: &tg.InputPeerChannel{
					ChannelID:  channel.ID,
					AccessHash: channel.AccessHash,
				},
				Writer:      writer,
				ExistingIDs: existingIDs,
			}
		}

		// 按频道顺序拉取历史消息（串行，避免并发 stdin 竞争）
		for _, username := range config.ChannelUsernames {
			var chCtx *ChannelCtx
			for _, ctx2 := range channelCtxMap {
				if strings.EqualFold(ctx2.Channel.Username, username) {
					chCtx = ctx2
					break
				}
			}
			if chCtx == nil {
				continue
			}
			if err := fetchHistoryInteractive(ctx, api, chCtx, *pageSize, *fetchHistory, *stopOnDup); err != nil {
				return err
			}
		}

		// 历史全部写完，取出缓冲期间的实时消息按频道追加
		rtMu.Lock()
		rtHistoryDone = true
		buffered := rtBuf
		rtBuf = nil
		rtMu.Unlock()

		if len(buffered) > 0 {
			LogInfo("追加写入缓冲期间收到的 %d 条实时消息...", len(buffered))
			for _, record := range buffered {
				if chCtx, ok := channelCtxMap[record.ChannelID]; ok {
					if writeErr := chCtx.Writer.Write(record); writeErr != nil {
						LogError(writeErr, "[%s] 写入缓冲消息失败", record.Channel)
					}
				}
			}
		}

		LogInfo("进入实时监听模式，等待新消息... (Ctrl+C 退出)")
		<-ctx.Done()
		LogInfo("收到退出信号，正在关闭...")
		return nil
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		LogError(err, "客户端运行异常")
	}
}
