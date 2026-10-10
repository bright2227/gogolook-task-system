package httpapi

import (
	"context"
	_ "embed"
	"net/http"

	"github.com/bright2227/gogolook-task-system/internal/delivery/http/controller"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
)

//go:embed openapi.json
var openAPISpec []byte

const swaggerUIHTML = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Gogolook Task API</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.33.1/swagger-ui.css" />
  </head>
  <body>
    <p>Swagger UI assets load from unpkg.com (swagger-ui-dist 5.33.1); viewing this page requires network access. The API specification is served locally at <a href="/openapi.json">/openapi.json</a>.</p>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5.33.1/swagger-ui-bundle.js"></script>
    <script>
      window.onload = () => SwaggerUIBundle({
        url: "/openapi.json",
        dom_id: "#swagger-ui",
        deepLinking: true,
        presets: [SwaggerUIBundle.presets.apis],
        layout: "BaseLayout"
      });
    </script>
  </body>
</html>`

func NewServer(taskUseCase *usecase.TaskUseCase, ready func(context.Context) error) http.Handler {
	api := controller.NewTaskController(taskUseCase, ready).Handler()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /openapi.json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(openAPISpec)
	})
	mux.HandleFunc("GET /docs", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(swaggerUIHTML))
	})
	mux.Handle("/", api)
	return mux
}
