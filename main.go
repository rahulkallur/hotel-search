package main

import (
	"fmt"
	"log"
	"time"

	"Search/controllers"
	"Search/routes"

	"github.com/gin-contrib/cors"
	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}
	e1 := gin.New()
	configCors := cors.DefaultConfig()
	controllers.StartConsumer()
	//Apply CORS middleware
	// // Update with your allowed origins
	configCors.AllowAllOrigins = true
	configCors.AllowCredentials = true
	configCors.AllowMethods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}
	configCors.AllowHeaders = []string{"*"}
	e1.Use(gzip.Gzip(gzip.DefaultCompression))
	e1.Use(cors.New(configCors))
	e1.GET("", func(c *gin.Context) {
		c.JSON(200, gin.H{"message": "Hello from Gateway!"})
	})
	e1.GET("/healthcheck", func(c *gin.Context) {
		c.JSON(200, gin.H{"message": "Hello from Gateway!"})
	})
	e1.Use(func(c *gin.Context) {
		if c.Request.TLS != nil {
			c.Writer.Header().Set("Strict-Transport-Security", fmt.Sprintf("max-age=%d; includeSubDomains; preload", int((60*24*time.Hour).Seconds())))
		}
		c.Next()
	})
	e1.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Content-Security-Policy", "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net/npm/swagger-ui-dist@<version>/swagger-ui-bundle.js'; img-src 'self'")
		c.Writer.Header().Set("X-XSS-Protection", "1; mode=block")
		c.Next()
	})
	routes.LoadSearchRequestRoute(e1)
	e1.Run(":8000")
}
