# PR 通知工作流使用文档

## 概述

这个 GitHub Actions 工作流用于在 Pull Request 创建或更新时自动发送通知到飞书群聊。当 PR 有指定的审查者时，会通过飞书 Webhook 发送包含 PR 详细信息的通知消息。

## 功能特性

- ⚡ 自动检测 PR 创建和更新事件
- 🔍 智能判断是否有审查者，无审查者时跳过通知
- 📱 发送格式化的飞书通知消息
- 🎯 包含 PR 标题、作者、仓库链接等详细信息
- 👥 显示所有指定的评审人列表

## 环境变量配置

### 必需的 Secrets

在 GitHub 仓库的 Settings > Secrets and variables > Actions 中配置以下环境变量：

#### 1. `GITHUB_TOKEN`
- **含义**: GitHub API 访问令牌
- **用途**: 用于调用 GitHub API 获取 PR 详细信息和审查者列表
- **配置**: 通常 GitHub Actions 会自动提供，无需手动配置
- **权限**: 需要 `pull_requests: read` 权限

#### 2. `FEISHU_WEBHOOK_URL`
- **含义**: 飞书群聊机器人的 Webhook URL
- **用途**: 用于发送通知消息到指定的飞书群聊
- **获取方式**: 
  1. 在飞书群聊中添加自定义机器人
  2. 复制生成的 Webhook URL
- **格式**: `https://open.feishu.cn/open-apis/bot/v2/hook/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`

## 实现步骤

### 1. 触发条件检查
- 监听 `pull_request` 事件的 `opened`（创建）和 `synchronize`（更新）动作
- 在 Ubuntu 最新版本的运行环境中执行

### 2. 审查者检查
```bash
# 调用 GitHub API 获取 PR 信息
# 统计 requested_reviewers 和 requested_teams 的总数
# 如果没有审查者，跳过通知流程
```

### 3. 获取评审人信息
```bash
# 提取所有 requested_reviewers 的用户名
# 格式化为逗号分隔的字符串
```

### 4. 构建通知消息
根据触发动作类型生成不同的消息：
- **创建 PR**: "⚡ PR 已创建 - 需要审查"
- **更新 PR**: "🔄 PR 已更新 - 需要重新审查"

消息包含以下信息：
- 🎯 PR 标题
- ✍️ 作者
- 🏠 仓库名称
- 🌐 PR 链接
- 🔍 评审人列表

### 5. 发送飞书通知
```bash
# 使用 curl 发送 POST 请求到飞书 Webhook
# 消息格式为 JSON，类型为 text
```

## 使用方法

### 1. 配置环境变量
1. 在 GitHub 仓库中进入 `Settings` > `Secrets and variables` > `Actions`
2. 添加 `FEISHU_WEBHOOK_URL` secret，值为飞书机器人的 Webhook URL

### 2. 部署工作流
1. 将 `pr-notify.yml` 文件放置在 `.github/workflows/` 目录下
2. 提交并推送到仓库

### 3. 测试功能
1. 创建一个新的 Pull Request 并指定审查者
2. 检查飞书群聊是否收到通知
3. 更新 PR 内容，验证更新通知是否正常发送

## 通知消息示例

```
⚡ PR 已创建 - 需要审查
🎯 标题: 添加用户认证功能
✍️ 作者: developer123
🏠 仓库: myorg/myproject
🌐 链接: https://github.com/myorg/myproject/pull/123
🔍 评审人: reviewer1,reviewer2
```

## 注意事项

1. **审查者检查**: 只有当 PR 指定了审查者时才会发送通知
2. **权限要求**: 确保 GitHub Token 有足够的权限访问 PR 信息
3. **网络连接**: 工作流需要能够访问 GitHub API 和飞书 Webhook URL
4. **消息格式**: 飞书通知使用 text 格式，支持换行符显示

## 故障排除

### 常见问题

1. **未收到通知**
   - 检查 PR 是否指定了审查者
   - 验证 `FEISHU_WEBHOOK_URL` 是否正确配置
   - 查看 GitHub Actions 运行日志

2. **API 调用失败**
   - 检查 `GITHUB_TOKEN` 权限
   - 确认网络连接正常

3. **消息格式异常**
   - 检查飞书 Webhook URL 是否有效
   - 验证 JSON 格式是否正确

