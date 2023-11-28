package nodeelement

import "awesomeProject/btreenode/datatype"

type NodeElement struct {
	key string
	obj *datatype.DataType
}

func (n *NodeElement) GetKey() string {
	return n.key
}

func (n *NodeElement) SetKey(key string) {
	n.key = key
}

func (n *NodeElement) GetObj() *datatype.DataType {
	return n.obj
}

func (n *NodeElement) SetObj(obj *datatype.DataType) {
	n.obj = obj
	n.key = obj.Key()
}

func NewNodeElement(key string, obj *datatype.DataType) *NodeElement {
	return &NodeElement{key: key, obj: obj}
}
