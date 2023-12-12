package http

import (
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

const (
	UnPlot   = 0
	Plotting = 1
	Ploted   = 2
	Fetching = 3
	Finish   = 4

	Idle    = 0
	Running = 1
	Offline = 2

	Err = -1
)

type TaskStatus int
type WorkerStatus int

type Task struct {
	IDHex              string
	CommitmentAtxIdHex string
	NumUnits           uint32
	Index              int
	Status             TaskStatus
	Host               string
	Port               int
}

type Worker struct {
	UUID          string
	ProviderID    uint32
	ProviderModel string
	Status        WorkerStatus
	Host          string
	Port          int
}

type TaskModel struct {
	gorm.Model
	Task `gorm:"embedded"`
}

type WorkerModel struct {
	gorm.Model
	Worker `gorm:"embedded"`
}

func (ts TaskStatus) String() string {
	switch ts {
	case UnPlot:
		return "UnPlot"
	case Plotting:
		return "Plotting"
	case Ploted:
		return "Ploted"
	case Fetching:
		return "Fetching"
	case Finish:
		return "Finish"
	case Err:
		return "Error"
	default:
		return "Unknown"
	}
}

func (ws WorkerStatus) String() string {
	switch ws {
	case Idle:
		return "Idle"
	case Running:
		return "Running"
	case Offline:
		return "Offline"
	case Err:
		return "Error"
	default:
		return "Unknown"
	}
}

func (w Worker) getUrl() string {
	return w.Host + ":" + string(w.Port)
}

func InitDatabase() *gorm.DB {
	db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}

	// 判断有没有完成建表
	db.AutoMigrate(&TaskModel{})
	db.AutoMigrate(&WorkerModel{})

	return db
}
