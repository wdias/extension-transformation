package main

import (
	"fmt"

	"github.com/kataras/iris"
)

func main() {
	app := iris.Default()
	app.Post("/extension/transformation/trigger/{extensionID:string}", func(ctx iris.Context) {
		extensionID := ctx.Params().Get("extensionID")
		fmt.Println("extensionID:", extensionID)
		ctx.JSON(iris.Map{
			"message": "OK",
		})
	})

	app.Get("/public/hc", func(ctx iris.Context) {
		ctx.JSON(iris.Map{
			"message": "OK",
		})
	})
	// listen and serve on http://0.0.0.0:8080.
	app.Run(iris.Addr(":8080"))
}
