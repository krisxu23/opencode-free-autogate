package m365

import (
	"net/http"
	"net/http/cookiejar"
)

// newStdCookieJar 独立小文件隔离 cookiejar 依赖：登录流需要会话 cookie
// （esctx/stsservicecookie 等），否则微软会把每步当新会话导致循环。
func newStdCookieJar() (http.CookieJar, error) {
	return cookiejar.New(nil)
}
