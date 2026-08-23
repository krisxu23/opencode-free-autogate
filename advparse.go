package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// advNode 是一条高级协议分享链接解析后的统一形态，
// 之后由内嵌 sing-box 实例把它变成一个本地 SOCKS5 端点。
type advNode struct {
	tag          string
	kind         string // vless | vmess | trojan | ss | hysteria2 | tuic
	server       string
	port         uint16
	uuid         string // vless / vmess / tuic 用户 ID
	password     string // trojan / ss / hysteria2 / tuic 密码
	method       string // ss 加密方法
	flow         string // vless xtls-rprx-vision 等
	tls          bool
	serverName   string
	allowInsecure bool
	realityPBK   string
	realitySID   string
	transport    string // ""(tcp) | ws | grpc | httpupgrade
	wsPath       string
	wsHost       string
	grpcService  string
	alpn         string
	obfsPassword string // hysteria2 obfs
}

var advancedSchemes = map[string]string{
	"vless":     "vless",
	"vmess":     "vmess",
	"trojan":    "trojan",
	"ss":        "ss",
	"hysteria2": "hysteria2",
	"hy2":       "hysteria2",
	"tuic":      "tuic",
}

func isAdvancedScheme(scheme string) (string, bool) {
	kind, ok := advancedSchemes[strings.ToLower(scheme)]
	return kind, ok
}

// parseAdvancedNode 把一条高级协议链接解析为统一的节点描述。
func parseAdvancedNode(link string) (*advNode, error) {
	schemeEnd := strings.Index(link, "://")
	if schemeEnd <= 0 {
		return nil, fmt.Errorf("缺少协议前缀")
	}
	kind, ok := isAdvancedScheme(link[:schemeEnd])
	if !ok {
		return nil, fmt.Errorf("非高级协议 %q", link[:schemeEnd])
	}

	switch kind {
	case "vmess":
		return parseVMessLink(link)
	case "ss":
		return parseSSLink(link)
	default:
		return parseUserinfoLink(kind, link)
	}
}

// parseUserinfoLink 处理 vless/trojan/hysteria2(hy2)/tuic 的标准 userinfo@host:port?query 形态。
func parseUserinfoLink(kind, link string) (*advNode, error) {
	parsed, err := url.Parse(strings.ReplaceAll(link, "hy2://", "hysteria2://"))
	if err != nil {
		return nil, fmt.Errorf("无法解析: %w", err)
	}
	if parsed.Host == "" || parsed.Port() == "" {
		return nil, fmt.Errorf("缺少主机或端口")
	}
	node := &advNode{tag: parsed.Fragment, kind: kind, server: parsed.Hostname()}
	port, err := strconv.Atoi(parsed.Port())
	if err != nil || port <= 0 || port > 65535 {
		return nil, fmt.Errorf("端口无效: %q", parsed.Port())
	}
	node.port = uint16(port)
	q := parsed.Query()

	switch kind {
	case "vless":
		node.uuid = parsed.User.Username()
		if node.uuid == "" {
			return nil, fmt.Errorf("缺少 UUID")
		}
		node.flow = q.Get("flow")
		node.tls = q.Get("security") == "tls" || q.Get("security") == "reality"
		if node.tls {
			node.serverName = firstNonEmpty(q.Get("sni"), q.Get("peer"))
			node.realityPBK = q.Get("pbk")
			node.realitySID = q.Get("sid")
		}
	case "trojan":
		node.password = parsed.User.Username()
		if node.password == "" {
			return nil, fmt.Errorf("缺少密码")
		}
		node.tls = true // trojan 必为 TLS
		node.serverName = firstNonEmpty(q.Get("sni"), q.Get("peer"), q.Get("host"))
		if q.Get("allowInsecure") == "1" || q.Get("insecure") == "1" {
			node.allowInsecure = true
		}
	case "hysteria2":
		auth := parsed.User.Username()
		if pass, ok := parsed.User.Password(); ok {
			auth = auth + ":" + pass
		}
		node.password = auth
		node.serverName = firstNonEmpty(q.Get("sni"), q.Get("peer"))
		node.alpn = q.Get("alpn")
		node.obfsPassword = q.Get("obfs-password")
		if q.Get("insecure") == "1" || q.Get("insecure") == "true" {
			node.allowInsecure = true
		}
	case "tuic":
		node.uuid = parsed.User.Username()
		node.password, _ = parsed.User.Password()
		node.serverName = firstNonEmpty(q.Get("sni"))
		node.alpn = firstNonEmpty(q.Get("alpn"), "h3")
		if q.Get("insecure") == "1" {
			node.allowInsecure = true
		}
	}

	node.transport = strings.ToLower(q.Get("type"))
	switch node.transport {
	case "", "tcp", "raw":
		node.transport = ""
	case "ws", "grpc", "httpupgrade":
		node.wsPath = q.Get("path")
		node.wsHost = firstNonEmpty(q.Get("host"))
		node.grpcService = q.Get("serviceName")
		node.serverName = firstNonEmpty(node.serverName, node.wsHost)
	case "http", "h2":
		node.transport = "httpupgrade" // 近似处理，多数机场 http 传输可用 httpupgrade 兼容
		node.wsPath = q.Get("path")
		node.wsHost = firstNonEmpty(q.Get("host"))
	default:
		return nil, fmt.Errorf("暂不支持的传输层 %q", q.Get("type"))
	}
	return node, nil
}

// parseVMessLink 支持 base64 JSON（v2rayN 风格）与 SIP002 两种写法。
func parseVMessLink(link string) (*advNode, error) {
	body := strings.TrimPrefix(link, "vmess://")

	// 形态一：base64 JSON
	if decoded, err := decodeBase64Any(body); err == nil && strings.HasPrefix(strings.TrimSpace(decoded), "{") {
		var raw struct {
			Add    string      `json:"add"`
			Port   interface{} `json:"port"`
			ID     string      `json:"id"`
			Aid    interface{} `json:"aid"`
			Scy    string      `json:"scy"`
			Net    string      `json:"net"`
			Type   string      `json:"type"`
			Host   string      `json:"host"`
			Path   string      `json:"path"`
			TLS    string      `json:"tls"`
			SNI    string      `json:"sni"`
			Ps     string      `json:"ps"`
		}
		if err := json.Unmarshal([]byte(decoded), &raw); err != nil {
			return nil, fmt.Errorf("vmess JSON 解析失败: %w", err)
		}
		port, err := toPort(raw.Port)
		if err != nil {
			return nil, err
		}
		if raw.Add == "" || raw.ID == "" {
			return nil, fmt.Errorf("vmess 缺少地址或 UUID")
		}
		node := &advNode{
			tag: raw.Ps, kind: "vmess", server: raw.Add, port: port,
			uuid: raw.ID, tls: strings.EqualFold(raw.TLS, "tls"),
			serverName: firstNonEmpty(raw.SNI, raw.Host),
			transport:  strings.ToLower(raw.Net),
		}
		switch node.transport {
		case "", "tcp", "raw":
			node.transport = ""
		case "ws":
			node.wsPath = raw.Path
			node.wsHost = raw.Host
		case "grpc":
			node.grpcService = raw.Path
		case "httpupgrade":
			node.wsPath = raw.Path
			node.wsHost = raw.Host
		case "http", "h2":
			node.transport = "httpupgrade"
			node.wsPath = raw.Path
			node.wsHost = raw.Host
		default:
			return nil, fmt.Errorf("vmess 暂不支持传输层 %q", raw.Net)
		}
		return node, nil
	}

	// 形态二：SIP002 vmess://uuid@host:port?...
	node, err := parseUserinfoLink("vmess", link)
	if err != nil {
		return nil, err
	}
	return node, nil
}

// parseSSLink 支持 ss://base64(method:pass)@host:port#tag 与 ss://base64(method:pass@host:port)#tag。
func parseSSLink(link string) (*advNode, error) {
	body := strings.TrimPrefix(link, "ss://")
	body = strings.TrimSpace(body)

	// 先剥掉名称片段（splitProxyInput 已处理过一次，这里兜底）。
	if i := strings.LastIndex(body, "#"); i >= 0 {
		body = body[:i]
	}

	wholeDecoded, wholeErr := decodeBase64Any(body)
	if at := strings.Index(body, "@"); at > 0 {
		// 形态一：userinfo 为 base64 的 method:pass
		userInfo := body[:at]
		hostPart := body[at+1:]
		if q := strings.Index(hostPart, "?"); q >= 0 {
			hostPart = hostPart[:q]
		}
		methodPass := userInfo
		if decoded, err := decodeBase64Any(userInfo); err == nil && strings.Contains(decoded, ":") {
			methodPass = decoded
		}
		method, pass := splitMethodPass(methodPass)
		hostParsed, err := url.Parse("socks5://" + hostPart)
		if err != nil || hostParsed.Port() == "" {
			return nil, fmt.Errorf("ss 缺少主机或端口")
		}
		if method == "" || pass == "" || hostParsed.Hostname() == "" {
			return nil, fmt.Errorf("ss 凭据或地址无效")
		}
		port, _ := strconv.Atoi(hostParsed.Port())
		return &advNode{kind: "ss", server: hostParsed.Hostname(), port: uint16(port), method: method, password: pass}, nil
	} else if wholeErr == nil && strings.Contains(wholeDecoded, "@") {
		// 形态二：整体 base64
		at := strings.Index(wholeDecoded, "@")
		methodPass := wholeDecoded[:at]
		hostPart := wholeDecoded[at+1:]
		if i := strings.LastIndex(hostPart, "#"); i >= 0 {
			hostPart = hostPart[:i]
		}
		method, pass := splitMethodPass(methodPass)
		hostParsed, err := url.Parse("socks5://" + hostPart)
		if err != nil || hostParsed.Hostname() == "" || hostParsed.Port() == "" {
			return nil, fmt.Errorf("ss 地址无效")
		}
		port, _ := strconv.Atoi(hostParsed.Port())
		return &advNode{kind: "ss", server: hostParsed.Hostname(), port: uint16(port), method: method, password: pass}, nil
	}
	return nil, fmt.Errorf("无法识别的 ss 链接格式")
}

func splitMethodPass(s string) (string, string) {
	if i := strings.Index(s, ":"); i > 0 {
		return s[:i], s[i+1:]
	}
	return "", s
}

func toPort(v interface{}) (uint16, error) {
	switch n := v.(type) {
	case float64:
		p := int(n)
		if p <= 0 || p > 65535 {
			return 0, fmt.Errorf("端口越界 %v", n)
		}
		return uint16(p), nil
	case string:
		p, err := strconv.Atoi(n)
		if err != nil || p <= 0 || p > 65535 {
			return 0, fmt.Errorf("端口无效 %q", n)
		}
		return uint16(p), nil
	default:
		return 0, fmt.Errorf("端口字段类型异常")
	}
}

func decodeBase64Any(s string) (string, error) {
	s = strings.TrimSpace(s)
	for _, enc := range []*base64.Encoding{
		base64.RawURLEncoding, base64.URLEncoding,
		base64.RawStdEncoding, base64.StdEncoding,
	} {
		if b, err := enc.DecodeString(s); err == nil {
			return string(b), nil
		}
	}
	return "", fmt.Errorf("base64 解码失败")
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
