# tg-reader

以真实用户客户端身份连接 Telegram，加入目标频道，支持翻页拉取历史消息并实时监听新消息，将消息结构化写入 JSONL 文件。

## 安装

需要 Go 1.21+。

```bash
go build -o tg-reader .
```

## 准备工作

前往 [my.telegram.org](https://my.telegram.org) 创建应用，获取 `API ID` 和 `API Hash`。

在项目目录创建 `.env`：

```env
TG_API_ID=12345678
TG_API_HASH=abcdef1234567890abcdef1234567890
TG_PHONE=+8613800138000
TG_CHANNEL_USERNAME=channel_a,channel_b
```

`TG_CHANNEL_USERNAME` 支持英文逗号分隔多个频道。也可通过 `-channels` flag 在运行时覆盖。

## 运行

```bash
./tg-reader [flags]
```

首次运行会要求输入手机验证码（及两步验证密码），认证成功后 session 保存至 `.td.session`，后续无需重新登录。

## 参数

| 参数          | 默认值             | 说明                                                                                                                      |
| ------------- | ------------------ | ------------------------------------------------------------------------------------------------------------------------- |
| `-channels`   | —                  | 频道用户名，逗号分隔，覆盖 `TG_CHANNEL_USERNAME`                                                                          |
| `-output`     | `./messages.jsonl` | 输出文件路径，多频道共用；被 `-output-dir` 覆盖时忽略                                                                     |
| `-output-dir` | —                  | 输出目录，每个频道写入 `{username}.jsonl`（无 username 的频道用数字 ID）                                                  |
| `-page-size`  | `100`              | 每次拉取历史消息条数，上限 100                                                                                            |
| `-history`    | `false`            | 跳过「是否拉取历史消息」的交互确认，直接开始拉取                                                                          |
| `-auto-stop`  | `false`            | 翻页遇到已存在的消息时自动停止，不弹确认；不加则用于手动回填历史空缺                                                      |
| `-proxy`      | —                  | SOCKS5 代理，格式 `socks5://[user:pass@]host:port`；留空依次读取 `ALL_PROXY` 环境变量，最终兜底 `socks5://localhost:7890` |

## 历史消息交互

拉取历史消息时按频道串行进行，每页完成后提示：

```
[Enter]  继续下一页
a        自动拉取全部剩余历史（不再询问）
q        停止翻页，写入已收集部分
```

首次运行（无已有记录）直接进入翻页，不询问是否拉取。有已有记录时默认询问（`[Y/n]`），或用 `-history` 跳过。

翻页遇到已存在的消息时默认询问是否停止（`[Y/n]`）；回答 `n` 则跳过该条继续，用于回填中间空缺的消息；`-auto-stop` 则直接停止。

## 输出格式

每行一条 JSON，字段说明：

```jsonc
{
  "id": 1234,
  "timestamp": "2026-03-30T10:00:00+08:00",
  "edited_at": "2026-03-30T10:05:00+08:00", // 有编辑时出现
  "text": "消息正文",
  "channel_id": 1001234567,
  "channel": "频道名称",
  "views": 8420,
  "forwards": 312,
  "replies": 47,
  "entities": [
    // 文本实体
    {"type": "url", "text": "https://..."},
    {"type": "mention", "text": "@user"},
    {"type": "hashtag", "text": "#tag"},
    {"type": "text_url", "text": "点击这里", "url": "https://..."},
  ],
  "media": {
    // 附件（如有）
    "type": "webpage", // photo/video/audio/voice/document/gif/sticker/webpage/poll/geo/contact/dice
    "url": "https://...",
    "title": "文章标题",
    "description": "摘要",
  },
  "forwarded_from": "原始频道名", // 转发消息时出现
  "post_author": "编辑签名", // 频道署名时出现
  "reply_to_id": 1200, // 回复消息时出现
}
```

所有时间字段均为 ISO 8601 格式，时区由 `TZ` 环境变量决定（如 `Asia/Hong_Kong`），未设置则默认香港时区（UTC+8）。
