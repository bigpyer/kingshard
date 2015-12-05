// Copyright 2015 The kingshard Authors. All rights reserved.
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

	"github.com/flike/kingshard/core/errors"
)

/* 更相减损法求最大公约数 */
func Gcd(ary []int) int {
	var i int
	min := ary[0]
	length := len(ary)
	/* 求最小值 */
	for i = 0; i < length; i++ {
		if ary[i] < min {
			min = ary[i]
		}
	}

	for {
		/* 是否求得公约数 */
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

func (n *Node) InitBalancer() {
	var sum int
	n.LastSlaveIndex = 0
	/* 获得最大公约数 */
	gcd := Gcd(n.SlaveWeights)

	/* 以最大公约数为步进，获取rrq元素总数 */
	for _, weight := range n.SlaveWeights {
		sum += weight / gcd
	}

	/* 根据rrq元素总数，分配节点下标作为rrq的值，生成rrq队列 */
	/* 这里的index和slave数组中的index一一对应 */
	n.RoundRobinQ = make([]int, 0, sum)
	for index, weight := range n.SlaveWeights {
		for j := 0; j < weight/gcd; j++ {
			n.RoundRobinQ = append(n.RoundRobinQ, index)
		}
	}

	//random order
	/* TODO 对rrq无规律随机排序一遍 */
	if 1 < len(n.SlaveWeights) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for i := 0; i < sum; i++ {
			/* 返回[0,n)的一个随机数 */
			x := r.Intn(sum)
			temp := n.RoundRobinQ[x]
			other := sum % (x + 1)
			n.RoundRobinQ[x] = n.RoundRobinQ[other]
			n.RoundRobinQ[other] = temp
		}
	}
}

func (n *Node) GetNextSlave() (*DB, error) {
	var index int
	if len(n.RoundRobinQ) == 0 {
		return nil, errors.ErrNoDatabase
	}
	if len(n.RoundRobinQ) == 1 {
		index = n.RoundRobinQ[0]
		return n.Slave[index], nil
	}

	queueLen := len(n.RoundRobinQ)
	index = n.RoundRobinQ[n.LastSlaveIndex]
	db := n.Slave[index]
	n.LastSlaveIndex++
	/* 到达rrq末尾后，将下标置为0，即rrq队列头，从rrq头开始轮训 */
	if queueLen <= n.LastSlaveIndex {
		n.LastSlaveIndex = 0
	}
	return db, nil
}
