package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
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
	SOCKS5_PROXY_ADDR = "localhost:7890"
	SESSION_FILE      = ".td.session"
	DEFAULT_OUTPUT    = "./messages.jsonl"
	DEFAULT_PAGE_SIZE = 50
)

// 全局共享 stdin reader，避免多次创建导致缓冲区数据丢失
var stdinReader = bufio.NewReader(os.Stdin)

// readLine 从标准输入读取一行并去除首尾空白
func readLine() (string, error) {
	val, err := stdinReader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(val), nil
}

// hktNow 返回香港时区 (+8) 的当前 ISO 时间字符串
func hktNow() string {
	loc := time.FixedZone("HKT", 8*3600)
	return time.Now().In(loc).Format(time.RFC3339)
}

// LogInfo 记录普通信息日志
func LogInfo(format string, v ...any) {
	fmt.Printf("[%s] [INFO] %s\n", hktNow(), fmt.Sprintf(format, v...))
}

// LogError 记录错误日志
func LogError(err error, format string, v ...any) {
	fmt.Printf("[%s] [ERROR] %s: %v\n", hktNow(), fmt.Sprintf(format, v...), err)
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
	AppID           int
	AppHash         string
	Phone           string
	ChannelUsername string
}

// loadConfig 读取并校验必需的环境变量
func loadConfig() (*AppConfig, error) {
	_ = godotenv.Load()

	appIDStr := os.Getenv("TG_API_ID")
	appHash := os.Getenv("TG_API_HASH")
	phone := os.Getenv("TG_PHONE")
	channelUsername := os.Getenv("TG_CHANNEL_USERNAME")

	if appIDStr == "" || appHash == "" || phone == "" || channelUsername == "" {
		return nil, errors.New(
			"缺少必须的环境变量: TG_API_ID, TG_API_HASH, TG_PHONE, TG_CHANNEL_USERNAME",
		)
	}

	appID, err := strconv.Atoi(appIDStr)
	if err != nil {
		return nil, fmt.Errorf("TG_API_ID 解析失败: %w", err)
	}

	return &AppConfig{
		AppID:           appID,
		AppHash:         appHash,
		Phone:           phone,
		ChannelUsername: channelUsername,
	}, nil
}

// EntityInfo 消息文本中的实体（链接、提及、话题标签、代码等）
type EntityInfo struct {
	Type string `json:"type"`
	Text string `json:"text"`
	URL  string `json:"url,omitempty"` // 仅 text_url 类型有效
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

// NewMessageWriter 打开或创建输出文件（追加模式）
func NewMessageWriter(path string) (*MessageWriter, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("打开输出文件失败 (%s): %w", path, err)
	}
	return &MessageWriter{file: file}, nil
}

// Write 将一条消息序列化为 JSON 写入文件（线程安全）
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

// Close 关闭文件
func (w *MessageWriter) Close() error {
	return w.file.Close()
}

// loadExistingIDs 读取已有 JSONL 输出文件，返回其中所有消息 ID 的集合
func loadExistingIDs(path string) (map[int]bool, error) {
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
	for scanner.Scan() {
		var record MessageRecord
		if jsonErr := json.Unmarshal(scanner.Bytes(), &record); jsonErr != nil {
			continue
		}
		existingIDs[record.ID] = true
	}
	return existingIDs, scanner.Err()
}

// extractEntities 从消息实体列表中提取有分析价值的条目
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

// extractMedia 从消息附件中提取关键元数据
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
		info := &MediaInfo{
			Type:         "poll",
			PollQuestion: m.Poll.Question.Text,
		}
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

// buildRecord 将 tg.Message 转换为完整的 MessageRecord
func buildRecord(msg *tg.Message, channelID int64, channelTitle string) MessageRecord {
	loc := time.FixedZone("HKT", 8*3600)
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

// printRecord 在终端打印一条消息，label 为来源标签（历史/实时）
func printRecord(record MessageRecord, label string) {
	text := record.Text
	runes := []rune(text)
	if len(runes) > 120 {
		text = string(runes[:120]) + "..."
	}
	text = strings.ReplaceAll(text, "\n", " ")
	fmt.Printf("  [%s] [%s] #%d: %s\n", label, record.Timestamp, record.ID, text)
}

// extractMessages 从 MessagesGetHistory 的返回值中提取消息列表
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

// oldestMessageID 从一页消息中找到最旧（ID 最小）的消息 ID，用于翻页偏移
func oldestMessageID(messages []tg.MessageClass) (int, bool) {
	for i := len(messages) - 1; i >= 0; i-- {
		if msg, ok := messages[i].(*tg.Message); ok {
			return msg.ID, true
		}
	}
	return 0, false
}

// joinChannelIfNeeded 若当前用户不是频道成员则尝试加入
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
// 每页展示后询问用户操作：
//
//	[Enter] 继续下一页
//	[a]     自动拉取全部剩余历史
//	[q]     停止翻页并将已收集部分写入
func fetchHistoryInteractive(
	ctx context.Context,
	api *tg.Client,
	peer tg.InputPeerClass,
	channel *tg.Channel,
	writer *MessageWriter,
	pageSize int,
	existingIDs map[int]bool,
) error {
	fmt.Print("\n是否拉取历史消息? [y/N]: ")
	answer, err := readLine()
	if err != nil {
		return err
	}
	if strings.ToLower(answer) != "y" {
		LogInfo("跳过历史消息拉取，直接进入实时监听")
		return nil
	}

	channelID := int64(channel.ID)
	channelTitle := channel.Title

	// 收集所有消息，最终倒序写入以保证时间正序
	var collected []MessageRecord

	offsetID := 0
	pageNum := 0
	autoFetch := false

	for {
		result, err := api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:     peer,
			Limit:    pageSize,
			OffsetID: offsetID,
		})
		if err != nil {
			return fmt.Errorf("拉取历史消息失败: %w", err)
		}

		messages := extractMessages(result)
		if len(messages) == 0 {
			break
		}

		pageNum++
		fmt.Printf("\n--- 第 %d 页（本页 %d 条）---\n", pageNum, len(messages))

		for _, msgClass := range messages {
			msg, ok := msgClass.(*tg.Message)
			if !ok {
				continue
			}

			if existingIDs[msg.ID] {
				record := buildRecord(msg, channelID, channelTitle)
				fmt.Printf("\n发现已存在的消息 #%d（%s），停止拉取? [Y/n]: ", msg.ID, record.Timestamp)
				input, err := readLine()
				if err != nil {
					return err
				}
				if strings.ToLower(input) != "n" {
					LogInfo("遇到已有消息，停止拉取历史")
					goto write
				}
				// 用户选择继续：跳过重复条目，但不中断翻页
				continue
			}

			record := buildRecord(msg, channelID, channelTitle)
			printRecord(record, "历史")
			collected = append(collected, record)
		}

		// 用最旧的消息 ID 作为下一页的偏移
		if minID, ok := oldestMessageID(messages); ok {
			offsetID = minID
		}

		// 消息数不足一页，说明已到顶部
		if len(messages) < pageSize {
			break
		}

		if autoFetch {
			continue
		}

		fmt.Print("\n[Enter] 继续  [a] 全部拉取  [q] 停止: ")
		input, err := readLine()
		if err != nil {
			return err
		}

		switch input {
		case "q":
			LogInfo("已停止翻页，将写入已收集的 %d 条消息", len(collected))
			goto write
		case "a":
			autoFetch = true
			LogInfo("切换为自动拉取全部历史...")
		}
	}

	LogInfo("已到达历史记录最顶部，共收集 %d 条消息", len(collected))

write:
	// 倒序遍历 collected（当前为新→旧），写入顺序变为旧→新（时间正序）
	for i := len(collected) - 1; i >= 0; i-- {
		if writeErr := writer.Write(collected[i]); writeErr != nil {
			LogError(writeErr, "写入消息失败")
		}
	}
	LogInfo("历史消息已按时间正序写入文件，共 %d 条", len(collected))

	return nil
}

func main() {
	outputPath := flag.String("output", DEFAULT_OUTPUT, "消息写入路径（JSONL 格式）")
	pageSize := flag.Int("page-size", DEFAULT_PAGE_SIZE, "历史消息每页条数")
	flag.Parse()

	config, err := loadConfig()
	if err != nil {
		LogError(err, "配置读取失败")
		return
	}

	writer, err := NewMessageWriter(*outputPath)
	if err != nil {
		LogError(err, "初始化输出文件失败")
		return
	}
	defer writer.Close()

	LogInfo("消息将写入: %s", *outputPath)

	// Telegram 使用原始 TCP 连接，需通过 SOCKS5 代理转发
	socks5Dialer, err := proxy.SOCKS5("tcp", SOCKS5_PROXY_ADDR, nil, proxy.Direct)
	if err != nil {
		LogError(err, "初始化 SOCKS5 代理失败")
		return
	}
	contextDialer := socks5Dialer.(proxy.ContextDialer)

	// targetChannel 在 Run 内部赋值后，供实时消息处理器使用
	var targetChannel *tg.Channel

	// 历史写入完成前，实时消息先缓冲，避免乱序写入文件
	var (
		rtMu          sync.Mutex
		rtBuf         []MessageRecord
		rtHistoryDone bool
	)

	dispatcher := tg.NewUpdateDispatcher()
	opts := telegram.Options{
		SessionStorage: &session.FileStorage{Path: SESSION_FILE},
		Resolver:       dcs.Plain(dcs.PlainOptions{Dial: contextDialer.DialContext}),
		UpdateHandler:  dispatcher,
	}

	client := telegram.NewClient(config.AppID, config.AppHash, opts)
	flow := auth.NewFlow(TerminalAuth{phone: config.Phone}, auth.SendCodeOptions{})

	// 注册实时新消息处理器
	dispatcher.OnNewChannelMessage(func(ctx context.Context, e tg.Entities, update *tg.UpdateNewChannelMessage) error {
		if targetChannel == nil {
			return nil
		}

		msg, ok := update.Message.(*tg.Message)
		if !ok || msg.Message == "" {
			return nil
		}

		peerChannel, ok := msg.PeerID.(*tg.PeerChannel)
		if !ok || peerChannel.ChannelID != targetChannel.ID {
			return nil
		}

		record := buildRecord(msg, int64(targetChannel.ID), targetChannel.Title)
		printRecord(record, "实时")

		rtMu.Lock()
		if !rtHistoryDone {
			rtBuf = append(rtBuf, record)
			rtMu.Unlock()
			return nil
		}
		rtMu.Unlock()

		if writeErr := writer.Write(record); writeErr != nil {
			LogError(writeErr, "写入实时消息失败")
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

		// 解析目标频道
		resolved, err := api.ContactsResolveUsername(ctx, &tg.ContactsResolveUsernameRequest{
			Username: config.ChannelUsername,
		})
		if err != nil {
			return fmt.Errorf("解析频道失败 (%s): %w", config.ChannelUsername, err)
		}

		for _, chat := range resolved.Chats {
			if ch, ok := chat.(*tg.Channel); ok {
				targetChannel = ch
				break
			}
		}
		if targetChannel == nil {
			return errors.New("未找到匹配的 Channel")
		}

		LogInfo("成功解析频道: %s (ID: %d)", targetChannel.Title, targetChannel.ID)

		// 加入频道（若尚未加入）
		if err := joinChannelIfNeeded(ctx, api, targetChannel); err != nil {
			return err
		}

		peer := &tg.InputPeerChannel{
			ChannelID:  targetChannel.ID,
			AccessHash: targetChannel.AccessHash,
		}

		// 加载已有消息 ID，用于断点续拉时的重复检测
		existingIDs, err := loadExistingIDs(*outputPath)
		if err != nil {
			return err
		}
		if len(existingIDs) > 0 {
			LogInfo("检测到已有 %d 条消息记录，翻页时遇到重复将提示停止", len(existingIDs))
		}

		// 交互式翻页拉取历史消息
		if err := fetchHistoryInteractive(ctx, api, peer, targetChannel, writer, *pageSize, existingIDs); err != nil {
			return err
		}

		// 历史写入完成，取出缓冲期间收到的实时消息并顺序追加
		rtMu.Lock()
		rtHistoryDone = true
		buffered := rtBuf
		rtBuf = nil
		rtMu.Unlock()

		if len(buffered) > 0 {
			LogInfo("追加写入缓冲期间收到的 %d 条实时消息...", len(buffered))
			for _, record := range buffered {
				if writeErr := writer.Write(record); writeErr != nil {
					LogError(writeErr, "写入缓冲消息失败")
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
