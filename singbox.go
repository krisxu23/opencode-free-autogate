package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/option"
)

// advancedBridge 把若干高级协议节点交给内嵌 sing-box：
// 每个节点分配一个 127.0.0.1 本地 SOCKS5 端口，网关其余部分
// （探活、竞速、手动保护）把它们当作普通 socks5 节点使用。
//
// 这里刻意用 sing-box 自己的 JSON 配置格式（而不是拼 Go 结构体）：
// JSON 字段是对外稳定契约，升级 sing-box 版本时不易被结构体改名打断。
const advancedBasePort = 21000

type advancedItem struct {
	link string // 原始分享链接（配置里的身份标识）
	node advNode
}

type advancedBridge struct {
	instance *box.Box
	Mapping  map[string]string // 本地地址 -> 展示名
	Links    map[string]string // 原始链接 -> 本地地址
}

// startAdvancedBridge 为每个节点启动一个本地 socks 入站。
func startAdvancedBridge(items []advancedItem) (*advancedBridge, error) {
	if len(items) == 0 {
		return nil, fmt.Errorf("没有可用的高级协议节点")
	}
	ports, err := allocatePorts(len(items))
	if err != nil {
		return nil, err
	}

	inbounds := make([]map[string]any, 0, len(items))
	outbounds := make([]map[string]any, 0, len(items))
	rules := make([]map[string]any, 0, len(items))

	bridge := &advancedBridge{
		Mapping: make(map[string]string, len(items)),
		Links:   make(map[string]string, len(items)),
	}
	for i := range items {
		item := &items[i]
		inTag := fmt.Sprintf("in-%d", i)
		outTag := fmt.Sprintf("out-%d", i)

		outbound, oerr := advancedOutboundJSON(outTag, &item.node)
		if oerr != nil {
			log.Printf("[高级] 跳过 %s: %v", displayNodeName(item.node), oerr)
			continue
		}
		inbounds = append(inbounds, map[string]any{
			"type":        "socks",
			"tag":         inTag,
			"listen":      "127.0.0.1",
			"listen_port": ports[i],
		})
		outbounds = append(outbounds, outbound)
		rules = append(rules, map[string]any{
			"inbound":  []string{inTag},
			"outbound": outTag,
		})

		localAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(ports[i])))
		bridge.Mapping[localAddr] = displayNodeName(item.node)
		bridge.Links[item.link] = localAddr
	}
	if len(outbounds) == 0 {
		return nil, fmt.Errorf("没有可用的高级协议节点")
	}

	cfg := map[string]any{
		"log":       map[string]any{"disabled": true},
		"inbounds":  inbounds,
		"outbounds": outbounds,
		"route":     map[string]any{"rules": rules},
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("生成 sing-box 配置失败: %w", err)
	}

	// include.Context 注册全部协议实现（含 QUIC 系需要 with_quic 构建标签）。
	ctx := include.Context(context.Background())
	var opts option.Options
	if err := opts.UnmarshalJSONContext(ctx, raw); err != nil {
		return nil, fmt.Errorf("sing-box 配置无效: %w", err)
	}
	instance, err := box.New(box.Options{Context: ctx, Options: opts})
	if err != nil {
		return nil, fmt.Errorf("sing-box 初始化失败: %w", err)
	}
	if err := instance.Start(); err != nil {
		_ = instance.Close()
		return nil, fmt.Errorf("sing-box 启动失败: %w", err)
	}
	bridge.instance = instance
	return bridge, nil
}

func (b *advancedBridge) Close() {
	if b != nil && b.instance != nil {
		_ = b.instance.Close()
	}
}

// maxAdvancedNodes 是桥接里高级节点的总量上限（含手动与节点池来源）。
const maxAdvancedNodes = 200

// ensureAdvancedBridge 把链接集合（手动 + 节点池订阅）合并进内嵌 sing-box。
// 集合有变化时重建实例：先成功启动新的，再原子替换旧的；失败则沿用旧桥接。
// 重建会重排本地端口，因此同步迁移槽位：手动节点直接保留，池节点重新探活。
func (g *gateway) ensureAdvancedBridge(ctx context.Context, freshLinks []string) {
	g.advMu.Lock()
	all := make([]string, 0, len(g.advSeen)+len(freshLinks))
	seenSet := make(map[string]struct{}, len(g.advSeen)+len(freshLinks))
	add := func(link string) {
		if _, dup := seenSet[link]; dup {
			return
		}
		seenSet[link] = struct{}{}
		all = append(all, link)
	}
	for link := range g.advSeen {
		add(link)
	}
	for _, link := range freshLinks {
		add(link)
	}
	if g.advBridge != nil && len(all) == len(g.advSeen) {
		g.advMu.Unlock()
		return // 没有新增，无需重建
	}
	if len(all) > maxAdvancedNodes {
		all = sampleStrings(all, maxAdvancedNodes)
		// advSeen 只登记真正进入桥接的链接：被抽样丢掉的保留「未知」身份，
		// 否则它们会被当成已处理而永远没有机会入选。
		seenSet = make(map[string]struct{}, len(all))
		for _, link := range all {
			seenSet[link] = struct{}{}
		}
	}

	items := make([]advancedItem, 0, len(all))
	for _, link := range all {
		node, err := parseAdvancedNode(link)
		if err != nil {
			continue // 订阅里的坏行直接跳过
		}
		items = append(items, advancedItem{link: link, node: *node})
	}
	if len(items) == 0 {
		g.advMu.Unlock()
		return // 没有可用的高级节点
	}

	newBridge, err := startAdvancedBridge(items)
	if err != nil {
		g.advMu.Unlock()
		log.Printf("[高级] sing-box (重)建失败，沿用现有配置: %v", err)
		return
	}
	old := g.advBridge
	var oldAddrs []string
	if old != nil {
		oldAddrs = make([]string, 0, len(old.Mapping))
		for addr := range old.Mapping {
			oldAddrs = append(oldAddrs, addr)
		}
	}
	g.advBridge = newBridge
	g.advSeen = seenSet

	manualSet := make(map[string]struct{}, len(g.manualAdv))
	for _, link := range g.manualAdv {
		manualSet[link] = struct{}{}
	}
	var manualSlots []slot
	var autoSlots []slot
	for link, local := range newBridge.Links {
		s, serr := slotFromRawURL("socks5://" + local)
		if serr != nil {
			continue
		}
		if _, isManual := manualSet[link]; isManual {
			manualSlots = append(manualSlots, s)
		} else {
			autoSlots = append(autoSlots, s)
		}
	}
	firstBuild := old == nil
	g.advMu.Unlock()

	// 旧实例在新实例接管后关闭，释放旧端口。
	if old != nil {
		old.Close()
	}

	// 迁移槽位：旧端口的槽位全部换绑到新端口。
	for _, addr := range oldAddrs {
		g.dropCustom(addr)
		g.removeManual(addr)
	}
	for _, s := range manualSlots {
		g.markManual(s.addr) // 手动节点无条件入池、永不自动移除
		g.addSlot(s, true)
	}
	if firstBuild {
		log.Printf("[高级] 内嵌 sing-box 已就绪：%d 个节点映射到本地端口 %d+（手动 %d）",
			len(newBridge.Mapping), advancedBasePort, len(manualSlots))
	} else {
		log.Printf("[高级] 节点更新：%d 个高级节点在线（新增候选 %d）", len(newBridge.Mapping), len(freshLinks))
	}

	// 池来源节点走探活门禁，健康才入池。
	if len(autoSlots) > 0 {
		added := 0
		for _, result := range probeSlots(ctx, g, autoSlots) {
			if result.slot.addr == "" || !result.ok {
				continue
			}
			if g.customCount() >= poolMaxAutoSlots {
				break
			}
			if g.addSlot(result.slot, true) {
				added++
				log.Printf("[高级+] %s (%dms)", result.slot.addr, result.latency.Milliseconds())
			}
		}
		log.Printf("[高级] 探活通过 %d/%d", added, len(autoSlots))
	}
}

func sampleStrings(items []string, limit int) []string {
	if len(items) <= limit {
		return items
	}
	out := make([]string, 0, limit)
	step := float64(len(items)) / float64(limit)
	for i := 0; i < limit; i++ {
		out = append(out, items[int(float64(i)*step)])
	}
	return out
}

// advancedOutboundJSON 按协议生成 sing-box 出站配置片段。
func advancedOutboundJSON(tag string, n *advNode) (map[string]any, error) {
	if n.server == "" || n.port == 0 {
		return nil, fmt.Errorf("缺少服务器地址或端口")
	}
	out := map[string]any{
		"tag":         tag,
		"server":      n.server,
		"server_port": n.port,
	}
	switch n.kind {
	case "vless":
		if n.uuid == "" {
			return nil, fmt.Errorf("缺少 UUID")
		}
		out["type"] = "vless"
		out["uuid"] = n.uuid
		if n.flow != "" {
			out["flow"] = n.flow
		}
		out["packet_encoding"] = "xudp"
		applyTLS(out, n, n.tls || n.realityPBK != "")
		applyTransport(out, n)
	case "vmess":
		if n.uuid == "" {
			return nil, fmt.Errorf("缺少 UUID")
		}
		out["type"] = "vmess"
		out["uuid"] = n.uuid
		out["security"] = "auto"
		applyTLS(out, n, n.tls)
		applyTransport(out, n)
	case "trojan":
		if n.password == "" {
			return nil, fmt.Errorf("缺少密码")
		}
		out["type"] = "trojan"
		out["password"] = n.password
		applyTLS(out, n, true)
		applyTransport(out, n)
	case "ss":
		if n.method == "" || n.password == "" {
			return nil, fmt.Errorf("缺少加密方法或密码")
		}
		out["type"] = "shadowsocks"
		out["method"] = n.method
		out["password"] = n.password
	case "hysteria2":
		out["type"] = "hysteria2"
		out["password"] = n.password
		applyTLS(out, n, true)
		if n.obfsPassword != "" {
			out["obfs"] = map[string]any{"type": "salamander", "password": n.obfsPassword}
		}
	case "tuic":
		out["type"] = "tuic"
		out["uuid"] = n.uuid
		out["password"] = n.password
		out["congestion_control"] = "bbr"
		if n.alpn == "" {
			n.alpn = "h3"
		}
		applyTLS(out, n, true)
	default:
		return nil, fmt.Errorf("暂不支持的协议 %q", n.kind)
	}
	return out, nil
}

func applyTLS(out map[string]any, n *advNode, enabled bool) {
	if !enabled {
		return
	}
	tls := map[string]any{
		"enabled":     true,
		"server_name": firstNonEmpty(n.serverName, n.server),
	}
	if n.allowInsecure {
		tls["insecure"] = true
	}
	if alpn := splitALPN(n.alpn); len(alpn) > 0 {
		tls["alpn"] = alpn
	}
	if n.realityPBK != "" {
		tls["reality"] = map[string]any{
			"enabled":    true,
			"public_key": n.realityPBK,
			"short_id":   n.realitySID,
		}
		tls["utls"] = map[string]any{"enabled": true, "fingerprint": "chrome"}
	} else if n.kind == "vless" || n.kind == "vmess" || n.kind == "trojan" {
		tls["utls"] = map[string]any{"enabled": true, "fingerprint": "chrome"}
	}
	out["tls"] = tls
}

func applyTransport(out map[string]any, n *advNode) {
	switch n.transport {
	case "ws":
		t := map[string]any{"type": "ws"}
		if n.wsPath != "" {
			t["path"] = n.wsPath
		}
		if n.wsHost != "" {
			t["headers"] = map[string]any{"Host": n.wsHost}
		}
		out["transport"] = t
	case "grpc":
		t := map[string]any{"type": "grpc"}
		if n.grpcService != "" {
			t["service_name"] = n.grpcService
		}
		out["transport"] = t
	case "httpupgrade":
		t := map[string]any{"type": "httpupgrade"}
		if n.wsPath != "" {
			t["path"] = n.wsPath
		}
		if n.wsHost != "" {
			t["host"] = n.wsHost
		}
		out["transport"] = t
	}
}

func splitALPN(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// allocatePorts 从 21000 起挑选 count 个当前空闲的本地端口。
func allocatePorts(count int) ([]uint16, error) {
	ports := make([]uint16, 0, count)
	for candidate := advancedBasePort; candidate < advancedBasePort+10000 && len(ports) < count; candidate++ {
		listener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(candidate)))
		if err != nil {
			continue // 端口被占用
		}
		_ = listener.Close()
		ports = append(ports, uint16(candidate))
	}
	if len(ports) < count {
		return nil, fmt.Errorf("本地端口不足")
	}
	return ports, nil
}

func displayNodeName(n advNode) string {
	name := firstNonEmpty(n.tag, fmt.Sprintf("%s:%d", n.server, n.port))
	return fmt.Sprintf("%s (%s)", name, strings.ToUpper(n.kind))
}
