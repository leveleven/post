package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type Server struct {
	db           *gorm.DB
	PendingTasks Queue
	Workers      map[string]Worker
}

func schedServer() {
	var server Server
	server.db = InitDatabase()
	r := gin.Default()
	r.GET("")
	r.POST("/worker/update", func(ctx *gin.Context) {
		var request struct {
			ID     uint `json:"id"`
			Status int  `json:"status"`
		}
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(400, gin.H{"error": err.Error()})
			return
		}
		server.updateWorkerStatus("id = ?", request.ID, request.Status)
	})
	r.POST("/worker/submit", func(ctx *gin.Context) {
		var request struct {
			UUID          string `json:"uuid"`
			ProviderID    uint32 `json:"provider"`
			ProviderModel string `json:"model"`
			Status        int    `json:"status"`
			Host          string `json:"host"`
			Port          int    `json:"port"`
		}
		if err := ctx.ShouldBind(&request); err != nil {
			ctx.JSON(400, gin.H{"error": err.Error()})
			return
		}
		worker := Worker{request.UUID, request.ProviderID, request.ProviderModel, WorkerStatus(request.Status), request.Host, request.Port}
		server.submitWorker(worker)
	})
	r.POST("/task/update", func(ctx *gin.Context) {
		var request struct {
			IDHex  string `json:"idhex"`
			Index  int    `json:"index"`
			Status int    `json:"status"`
		}
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(400, gin.H{"error": err.Error()})
			return
		}
		server.updateTaskStatus(request.IDHex, request.Index, request.Status)
	})
	r.POST("/task/create", func(ctx *gin.Context) {
		var request struct {
			IDHex    string `json:"idhex"`
			NumUnits int    `json:"numUnits"`
		}
		if err := ctx.BindJSON(&request); err != nil {
			ctx.JSON(400, gin.H{"error": err.Error()})
			return
		}
		if err := server.createTask(request.IDHex, request.NumUnits); err != nil {
			ctx.JSON(200, gin.H{"message": err.Error()})
			return
		}
		ctx.JSON(200, gin.H{"message": "created task"})
	})
	// 启动状态管理

	// 启动任务管理

	r.Run()
}

// 分解任务，写入数据库
func (s *Server) createTask(idHex string, numUnits int) error {
	// 查数据库
	result := &TaskModel{}
	s.db.Where("IDHex = ?", idHex).First(result)

	// 判断是否存在
	if result != (&TaskModel{}) {
		return fmt.Errorf("task is exist.")
	}

	plot_number := numUnits * 16
	commitmentatxid := "9eebff023abb17ccb775c602daade8ed708f0a50d3149a42801184f5b74f2865"
	for i := 0; i < plot_number; i++ {
		task := Task{
			IDHex:              idHex,
			CommitmentAtxIdHex: commitmentatxid,
			NumUnits:           uint32(numUnits),
			Index:              i,
			Status:             0,
			Host:               "192.168.1.1",
			Port:               5678,
		}
		s.db.Create(&TaskModel{Task: task})
		// 任务进入队列，等待下发
		s.PendingTasks.Join(task)
	}

	return nil
}

func (s *Server) handleStatus() {
	// 任务状态

	// worker状态
	var workers []WorkerModel
	go func(ws []WorkerModel) {
		for {
			s.db.Where("status > ?", 0).Find(&ws)
			for _, w := range ws {
				s.checkWorkerAlive(w)
			}
			time.Sleep(10 * time.Second)

		}
	}(workers)
}

func (s *Server) handleTask() {
	// 分发任务
	go func(ts *Queue) {
		for {
			if ts.Len() != 0 {
				// 获取任务
				t, err := ts.Pop()
				if err != nil {
					continue
				}
				jsonData, err := json.Marshal(t)
				if err != nil {
					fmt.Println("cant encoding json:", err)
				}
				// 获取worker
				for _, w := range s.Workers {
					if w.Status == 0 {
						if err := distributeTask(jsonData, w.getUrl()); err != nil {
							fmt.Println("cant distribute task to worker:", w.getUrl(), w.ProviderID)
							continue
						}
						// worker状态更新
						s.updateWorkerStatus("uuid = ?", w.UUID, 1)
						break
					}
				}
			}
		}
	}(&s.PendingTasks)
}

func distributeTask(data []byte, url string) error {
	request, err := http.NewRequest("POST", url, bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("cant create request:", err)
	}
	request.Header.Set("Content-Type", "application/json")
	client := http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("cant sending request:", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return fmt.Errorf("cant reading response body:", err)
	}

	fmt.Println(string(body))
	return nil
}

func (s *Server) checkWorkerAlive(w WorkerModel) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", w.Host, w.Port), time.Second)
	if err != nil {
		// 离线报警
		s.updateWorkerStatus("id = ?", w.ID, 2)
		s.Workers[w.UUID] = Worker{
			UUID:          w.UUID,
			ProviderID:    w.ProviderID,
			ProviderModel: w.ProviderModel,
			Status:        2,
			Host:          w.Host,
			Port:          w.Port,
		}
	}
	defer conn.Close()
}

func (s *Server) updateWorkerStatus(item interface{}, value interface{}, status int) error {
	// return s.updateStatus(WorkerModel{}, status, "id = ?", value)
	return s.updateStatus(WorkerModel{}, status, item, value)
}

func (s *Server) updateTaskStatus(idhex string, index int, status int) error {
	return s.updateStatus(TaskModel{}, status, "idhex = ? AND index = ?", idhex, index)
}

func (s *Server) updateStatus(table interface{}, status int, query interface{}, args ...interface{}) error {
	updatedRows := s.db.Model(&table).Where(query, args).Update("status", status).RowsAffected
	if s.db.Error != nil {
		fmt.Errorf("update failed: %v\n", s.db.Error)
	} else if updatedRows == 0 {
		fmt.Errorf("can't not find record")
	}
	return nil
}

func (s *Server) add(table interface{}) error {
	addRows := s.db.Create(&table)
	if addRows.Error != nil {
		fmt.Errorf("add failed: %v\n", addRows.Error)
	}
	return nil
}

// worker注册
func (s *Server) submitWorker(w Worker) error {
	if err := s.add(w); err != nil {
		return err
	}
	s.Workers[w.UUID] = w
	// s.Workers = append(s.Workers, w)
	return nil
}
