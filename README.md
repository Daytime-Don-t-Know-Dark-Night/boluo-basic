<p align="center">
	<img alt="logo" src="https://oscimg.oschina.net/oscnet/up-d3d0a9303e11d522a06cd263f3079027715.png">
</p>
<h1 align="center" style="margin: 30px 0 30px; font-weight: bold;">Basic</h1>
<h4 align="center">基于Maven构建的通用子模块</h4>
<p align="center">
	<a href="https://gitee.com/yuanyingshuo/pic-md1/raw/master/1e4eccdb0db03cde.jpg">
        <img src="https://gitee.com/y_project/RuoYi-Vue/badge/star.svg?theme=dark"></a>
	<a href="https://gitee.com/yuanyingshuo/pic-md1/raw/master/1e4eccdb0db03cde.jpg">
        <img src="https://img.shields.io/badge/RuoYi-v3.8.1-brightgreen.svg"></a>
	<a href="https://gitee.com/yuanyingshuo/pic-md1/raw/master/1e4eccdb0db03cde.jpg">
        <img src="https://img.shields.io/github/license/mashape/apistatus.svg"></a>
</p>






## 模块简介

可作为以后所有maven项目的子模块使用，包含一些常用的自定义方法及工具类，减少不同项目中的重复代码。

## 开发原则

- 所提供接口应尽量避免业务逻辑，专注于技术需求

- 原则上不允许在父仓库中对子模块进行修改

## 使用规范

- 子模块更新代码

- 父仓库主分支拉取子模块更新的代码, 并且提交到项目中, 保持父仓库中子模块版本号与子模块最新版本号一致

- 父仓库其他分支合并主分支代码

  ```shell
  git submodule update
  ```

  

## 演示图

此处父仓库使用: [parent](https://github.com/Daytime-Don-t-Know-Dark-Night/parent)

此处子仓库使用: [boluo-basic](https://github.com/Daytime-Don-t-Know-Dark-Night/boluo-basic)



### 父仓库添加子模块

<table>
    <tr>
        <td><img title="在父仓库中添加子仓库依赖" src="https://gitee.com/yuanyingshuo/pic-md1/raw/master/master添加子模块.png"/></td>
        <td><img title="在需要使用的模块中引入basic" src="https://gitee.com/yuanyingshuo/pic-md1/raw/master/在需要使用的模块中引入basic.png"/></td>
    </tr>
    <tr>
        <td><img title="使用basic子模块中的方法" src="https://gitee.com/yuanyingshuo/pic-md1/raw/master/使用basic子模块中的方法.png"/></td>
        <td><img title="哈哈哈这只是一个分隔符" src="https://gitee.com/yuanyingshuo/pic-md1/raw/master/哈哈哈这只是一个分隔符.png"/></td>
	</tr>
</table>



### 父仓库非master分支更新子模块

<table>
    <tr>
        <td><img title="父仓库其他分支更新子模块" src="https://gitee.com/yuanyingshuo/pic-md1/raw/master/其他分支合并master分支代码且更新子模块.png"/></td>
        <td><img title="父仓库其他分支使用子模块中的方法" src="https://gitee.com/yuanyingshuo/pic-md1/raw/master/父仓库其他分支使用子模块中的方法.png"/></td>
    </tr>
</table>



### 父仓库更新子模块版本

<table>
    <tr>
        <td><img title="basic子模块更新之前的版本" src="https://gitee.com/yuanyingshuo/pic-md1/raw/master/basic子模块更新之前的版本.png"/></td>
        <td><img title="父仓库中子模块版本也和子模块最新版本一致" src="https://gitee.com/yuanyingshuo/pic-md1/raw/master/父仓库中查看子模块版本也和子模块最新版本一致.png"/></td>
    </tr>
    <tr>
        <td><img title="basic子模块更新之后的版本" src="https://gitee.com/yuanyingshuo/pic-md1/raw/master/basic子模块更新之后的版本.png"/></td>
        <td><img title="父仓库中先更新子模块最新代码" src="https://gitee.com/yuanyingshuo/pic-md1/raw/master/父仓库中先更新子模块主分支代码.png"/></td>
    </tr>
    <tr>
        <td><img title="此时父仓库中子模块版本号已和子模块最新版本号一致" src="https://gitee.com/yuanyingshuo/pic-md1/raw/master/此时父仓库中子模块版本号已和子模块最新版本号一致.png"/></td>
        <td><img title="将父仓库中最新版本的basic提交" src="https://gitee.com/yuanyingshuo/pic-md1/raw/master/将最新版本basic提交.png"/></td>
    </tr>
    <tr>
        <td><img title="父仓库提交新版basic之前的子模块版本" src="https://gitee.com/yuanyingshuo/pic-md1/raw/master/父仓库提交新版basic之前的子模块版本.png"/></td>
        <td><img title="父仓库提交新版basic之后的子模块版本" src="https://gitee.com/yuanyingshuo/pic-md1/raw/master/父仓库提交新版basic之后的子模块版本.png"/></td>
    </tr>
</table>
