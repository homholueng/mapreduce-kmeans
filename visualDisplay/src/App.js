import React, { Component } from 'react';
import logo from './logo.svg';
import './App.css';
import {Table} from 'antd';

let arr=str.split("),");
let length=arr.length-1;
console.log(length);
let dataSource=[];
for(let i=0;i<length;i++){
  let a=arr[i].split("', '");
  dataSource[i]={};
  dataSource[i].key=i.toString();
  dataSource[i].id=a[1];
  dataSource[i].content=a[2];
};


const columns = [{
  title: 'id',
  dataIndex: 'id',
  key: 'id',
  sorter: (a, b) => a.id - b.id,
        filters: [
        { text: '1', value: '1' },
        { text: '2', value: '2' },
        { text: '3', value: '3' },
        { text: '4', value: '4' },        
        { text: '5', value: '5' },        
        { text: '6', value: '6' },        
        { text: '7', value: '7' },
        { text: '8', value: '8' },
        { text: '9', value: '9' },
      ],
  onFilter: (value, record) => record.id.includes(value),
}, {
  title: '内容',
  dataIndex: 'content',
  key: 'content',
}];

class App extends Component {
  render() {
    return (
      <div className="App">
        <Table dataSource={dataSource} columns={columns}/>
      </div>
    );
  }
}

export default App;