package main

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
)

type proxyParseError struct {
	Line  int
	Input string
	Err   error
}

func (e proxyParseError) Error() string {
	if e.Input == "" {
		return fmt.Sprintf("第 %d 行: %v", e.Line, e.Err)
	}
	return fmt.Sprintf("第 %d 行 %q: %v", e.Line, e.Input, e.Err)
}

// splitProxyInput 拆分输入：先按换行分行，去掉 # 名称片段（片段里常有逗号、
// 空格等干扰字符），再按逗号拆分同一行内的多个节点。
func splitProxyInput(raw string) []string {
	lines := strings.FieldsFunc(raw, func(r rune) bool { return r == '\n' || r == '\r' })
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if i := strings.Index(line, "#"); i >= 0 {
			line = strings.TrimSpace(line[:i])
		}
		for _, field := range strings.Split(line, ",") {
			field = strings.TrimSpace(field)
			if field != "" {
				out = append(out, field)
			}
		}
	}
	return out
}

// ParseProxyInput 把多行/逗号分隔的节点输入转换为网关可用的代理 URL 列表。
// 支持 http/https/socks5/socks5h 以及 socks://base64@host:port#名称 分享链接；
// 返回去重后的可用列表和逐条解析错误。
func ParseProxyInput(raw string) ([]string, []proxyParseError) {
	var good []string
	var bad []proxyParseError
	seen := make(map[string]struct{})
	for i, line := range splitProxyInput(raw) {
		normalized, err := normalizeProxyLine(line)
		if err != nil {
			bad = append(bad, proxyParseError{Line: i + 1, Input: line, Err: err})
			continue
		}
		if _, dup := seen[normalized]; dup {
			continue
		}
		seen[normalized] = struct{}{}
		good = append(good, normalized)
	}
	return good, bad
}

var unsupportedProxySchemes = map[string]string{
	"hysteria": "Hysteria(v1)",
	"anytls":   "AnyTLS",
}

// advancedSchemes 在 advparse.go 中定义：这些协议经内嵌 sing-box 转换为本地
// SOCKS5 端点后使用，链接本身原样保留在配置里。
func normalizeProxyLine(line string) (string, error) {
	line = strings.TrimSpace(line)
	if i := strings.Index(line, "#"); i >= 0 {
		line = line[:i]
	}
	line = strings.TrimSpace(line)
	schemeEnd := strings.Index(line, "://")
	if schemeEnd <= 0 {
		return "", fmt.Errorf("缺少协议前缀（应为 http(s)://、socks5:// 或分享链接）")
	}
	scheme := strings.ToLower(line[:schemeEnd])
	switch scheme {
	case "socks5", "socks5h", "http", "https":
		parsed, err := url.Parse(line)
		if err != nil {
			return "", fmt.Errorf("无法解析: %w", err)
		}
		if parsed.Host == "" {
			return "", fmt.Errorf("缺少主机地址")
		}
		if parsed.Port() == "" {
			return "", fmt.Errorf("缺少端口")
		}
		return parsed.String(), nil
	case "socks":
		return convertSharedSOCKS(line)
	default:
		if kind, ok := isAdvancedScheme(scheme); ok {
			// 校验可解析，链接原样保留：启动时由内嵌 sing-box 转成本地 socks 端口。
			if _, err := parseAdvancedNode(line); err != nil {
				return "", fmt.Errorf("%s 链接无效: %w", strings.ToUpper(kind), err)
			}
			return line, nil
		}
		if label, known := unsupportedProxySchemes[scheme]; known {
			return "", fmt.Errorf("%s 协议暂不支持", label)
		}
		return "", fmt.Errorf("不支持的协议 %q", scheme)
	}
}

// convertSharedSOCKS 处理形如 socks://base64(user:pass)@host:port 的分享链接，
// 解码出账号密码后重建为标准 socks5://user:pass@host:port。
func convertSharedSOCKS(line string) (string, error) {
	rest := strings.TrimPrefix(strings.TrimPrefix(line, "socks:"), "//")
	at := strings.Index(rest, "@")
	if at < 0 {
		parsed, err := url.Parse("socks5://" + rest)
		if err != nil || parsed.Host == "" || parsed.Port() == "" {
			return "", fmt.Errorf("缺少 @用户名:密码 或端口")
		}
		return parsed.String(), nil
	}
	userInfo := rest[:at]
	hostPart := rest[at+1:]
	hostParsed, err := url.Parse("socks5://" + hostPart)
	if err != nil || hostParsed.Host == "" {
		return "", fmt.Errorf("主机地址无效")
	}
	if hostParsed.Port() == "" {
		return "", fmt.Errorf("缺少端口")
	}
	username, password, ok := decodeShareCredential(userInfo)
	if !ok {
		return "", fmt.Errorf("凭据 base64 解码失败")
	}
	rebuilt := url.URL{Scheme: "socks5", User: url.UserPassword(username, password), Host: hostParsed.Host}
	return rebuilt.String(), nil
}

// decodeShareCredential 尝试常见 base64 变体解码 “user:pass”。
func decodeShareCredential(userInfo string) (string, string, bool) {
	userInfo = strings.TrimSpace(userInfo)
	encodings := []*base64.Encoding{
		base64.StdEncoding,
		base64.RawStdEncoding,
		base64.URLEncoding,
		base64.RawURLEncoding,
	}
	for _, enc := range encodings {
		decoded, err := enc.DecodeString(userInfo)
		if err != nil {
			continue
		}
		text := string(decoded)
		if i := strings.Index(text, ":"); i > 0 {
			return text[:i], text[i+1:], true
		}
	}
	// 非 base64：退回明文 user:pass。
	if i := strings.Index(userInfo, ":"); i > 0 {
		return userInfo[:i], userInfo[i+1:], true
	}
	return "", "", false
}
