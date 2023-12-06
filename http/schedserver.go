package http

import (
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type Server struct {
	db    *gorm.DB
	queue []Task
}

func main() {
	r := gin.Default()
	r.GET("")
}

// 分解任务，写入数据库
func (s *Server) handlerTask(idHex string, numUnits int) {
	plot_number := numUnits * 16
	commitmentatxid := "9eebff023abb17ccb775c602daade8ed708f0a50d3149a42801184f5b74f2865"
	for i := 0; i < plot_number; i++ {
		task := Task{
			IDHex:              idHex,
			CommitmentAtxIdHex: commitmentatxid,
			NumUnits:           numUnits,
			Index:              i,
			Status:             0,
			Host:               "192.168.1.1",
			Port:               5678,
		}
		s.db.Create(&TaskModel{Task: task})
		// 任务进入队列，等待下发
		s.queue = append(s.queue, task)
	}
}

// worker注册
func (s *Server) submitWorker() {

}

// 检测worker状态

// 分发任务
