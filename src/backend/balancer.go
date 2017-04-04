// Copyright 2016 The kingshard Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package backend

import (
	"math/rand"
	"time"

	"kingshard/core/errors"
)

//
// 获取指定数组的最大公约数
//
func Gcd(ary []int) int {
	var i int
	min := ary[0]
	length := len(ary)
	for i = 0; i < length; i++ {
		if ary[i] < min {
			min = ary[i]
		}
	}

	for {
		isCommon := true
		for i = 0; i < length; i++ {
			if ary[i]%min != 0 {
				isCommon = false
				break
			}
		}
		if isCommon {
			break
		}
		min--
		if min < 1 {
			break
		}
	}
	return min
}

//
// 计算sum, 然后通过 RoundRobinQ 来按照weight随机选择后端节点
//
func (n *Node) InitBalancer() {
	var sum int
	n.LastSlaveIndex = 0
	gcd := Gcd(n.SlaveWeights)

	for _, weight := range n.SlaveWeights {
		sum += weight / gcd
	}

	// RoundRobinQ 将每一个weight作为一个元素，存放在RoundRobinQ中
	n.RoundRobinQ = make([]int, 0, sum)
	for index, weight := range n.SlaveWeights {
		for j := 0; j < weight/gcd; j++ {
			n.RoundRobinQ = append(n.RoundRobinQ, index)
		}
	}

	// 随机排序
	//random order
	if 1 < len(n.SlaveWeights) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := 0; i < sum; i++ {
			x := r.Intn(sum)
			temp := n.RoundRobinQ[x]
			other := sum % (x + 1)
			n.RoundRobinQ[x] = n.RoundRobinQ[other]
			n.RoundRobinQ[other] = temp
		}
	}
}

// 获取下一个DB
func (n *Node) GetNextSlave() (*DB, error) {
	var index int
	queueLen := len(n.RoundRobinQ)
	if queueLen == 0 {
		return nil, errors.ErrNoDatabase
	}

	// 只有一个，直接返回
	if queueLen == 1 {
		index = n.RoundRobinQ[0]
		return n.Slave[index], nil
	}

	// 获取一个Index
	n.LastSlaveIndex = n.LastSlaveIndex % queueLen
	index = n.RoundRobinQ[n.LastSlaveIndex]

	if len(n.Slave) <= index {
		return nil, errors.ErrNoDatabase
	}

	db := n.Slave[index]
	n.LastSlaveIndex++
	n.LastSlaveIndex = n.LastSlaveIndex % queueLen
	return db, nil
}
