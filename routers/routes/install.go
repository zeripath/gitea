// Copyright 2020 The Gitea Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package routes

import (
	"fmt"
	"net/http"
	"path"

	"code.gitea.io/gitea/modules/log"
	"code.gitea.io/gitea/modules/public"
	"code.gitea.io/gitea/modules/setting"
	"code.gitea.io/gitea/modules/templates"
	"code.gitea.io/gitea/modules/web"
	"code.gitea.io/gitea/modules/web/middleware"
	"code.gitea.io/gitea/routers"
	"code.gitea.io/gitea/services/forms"

	"gitea.com/go-chi/session"
)

func installRecovery() func(next http.Handler) http.Handler {
	var rnd = templates.HTMLRenderer()
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			defer func() {
				// Why we need this? The first recover will try to render a beautiful
				// error page for user, but the process can still panic again, then
				// we have to just recover twice and send a simple error page that
				// should not panic any more.
				defer func() {
					if err := recover(); err != nil {
						combinedErr := fmt.Sprintf("PANIC: %v\n%s", err, string(log.Stack(2)))
						log.Error(combinedErr)
						if setting.IsProd() {
							http.Error(w, http.StatusText(500), 500)
						} else {
							http.Error(w, combinedErr, 500)
						}
					}
				}()

				if err := recover(); err != nil {
					combinedErr := fmt.Sprintf("PANIC: %v\n%s", err, string(log.Stack(2)))
					log.Error("%v", combinedErr)

					lc := middleware.Locale(w, req)
					var store = dataStore{
						Data: templates.Vars{
							"Language":       lc.Language(),
							"CurrentURL":     setting.AppSubURL + req.URL.RequestURI(),
							"i18n":           lc,
							"SignedUserID":   int64(0),
							"SignedUserName": "",
						},
					}

					w.Header().Set(`X-Frame-Options`, `SAMEORIGIN`)

					if !setting.IsProd() {
						store.Data["ErrorMsg"] = combinedErr
					}
					err = rnd.HTML(w, 500, "status/500", templates.BaseVars().Merge(store.Data))
					if err != nil {
						log.Error("%v", err)
					}
				}
			}()

			next.ServeHTTP(w, req)
		})
	}
}

// InstallRoutes registers the install routes
func InstallRoutes() *web.Route {
	r := web.NewRoute()
	for _, middle := range commonMiddlewares() {
		r.Use(middle)
	}

	r.Use(public.AssetsHandler(&public.Options{
		Directory: path.Join(setting.StaticRootPath, "public"),
		Prefix:    "/assets",
	}))

	r.Use(session.Sessioner(session.Options{
		Provider:       setting.SessionConfig.Provider,
		ProviderConfig: setting.SessionConfig.ProviderConfig,
		CookieName:     setting.SessionConfig.CookieName,
		CookiePath:     setting.SessionConfig.CookiePath,
		Gclifetime:     setting.SessionConfig.Gclifetime,
		Maxlifetime:    setting.SessionConfig.Maxlifetime,
		Secure:         setting.SessionConfig.Secure,
		SameSite:       setting.SessionConfig.SameSite,
		Domain:         setting.SessionConfig.Domain,
	}))

	r.Use(installRecovery())
	r.Use(routers.InstallInit)
	r.Get("/", routers.Install)
	r.Post("/", web.Bind(forms.InstallForm{}), routers.InstallPost)
	r.NotFound(func(w http.ResponseWriter, req *http.Request) {
		http.Redirect(w, req, setting.AppURL, 302)
	})
	return r
}
