package routes

import (
	"Search/controllers"
	"Search/services"

	"github.com/gin-gonic/gin"
)

func LoadSearchRequestRoute(router *gin.Engine) {
	repo := services.NewSearchRequestRepository()
	controller := controllers.NewSearchRequestController(repo)
	onboarding := router.Group("Search")
	onboarding.POST("SearchRequest", controller.SearchRequestMapper)
}
