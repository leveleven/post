package http

import "fmt"

type Queue struct {
	items []interface{}
}

func (q *Queue) Join(item interface{}) {
	q.items = append(q.items, item)
}

func (q *Queue) Pop() (interface{}, error) {
	if len(q.items) == 0 {
		return 0, fmt.Errorf("queue is empty")
	}

	item := q.items[0]
	q.items = q.items[1:]

	return item, nil
}

func (q *Queue) Len() int {
	return len(q.items)
}
