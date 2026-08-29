package main

// 供应商模型：网关目前有两个上游供应商。
//
// 参考 OmniRoute（provider→connection→api_key 三层）与 9router
// （provider→账号→baseUrl）的建模方式，但按本网关的现实裁剪：
// 两个供应商形态各异（opencode.ai/zen 是 OpenAI 兼容通道，Cline 是
// OAuth 账号制反代），引入注册表+执行器的间接成本大于收益。这里把
// "供应商"收敛成三个统一的正交面，新供应商接入时沿这三面扩展：
//
//	1. 出口策略（本文件）：每供应商独立决定走节点池多 IP 轮询还是
//	   直连——用户按供应商开关（PROXY_POOL_OPENCODE / PROXY_POOL_CLINE，
//	   GUI 两个供应商页各一个勾选框）；
//	2. 健康记账：凡走池的供应商共用 exitTracker/全局熔断/共享
//	   Transport 连接池（gateway 既有设施，Cline 轮询出口同样记账）；
//	3. 模型目录：/v1/models 合并展示（models.go/cline_models.go 既有）。

const (
	providerOpenCode = "opencode"
	providerCline    = "cline"
)

// poolEnabledFor 报告指定供应商是否启用节点池 IP 出口。
// 未启用 = 该供应商全部请求直连（镜像/账号等供应商内部逻辑不受影响）。
func (c config) poolEnabledFor(provider string) bool {
	if provider == providerCline {
		return c.clinePoolEnabled
	}
	return !c.opencodePoolOff
}
