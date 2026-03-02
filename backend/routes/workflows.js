const express = require('express');
const router = express.Router();
const Workflow = require('../models/workflow');
const workflowExecutor = require('../services/workflowService');
const kafkaService = require('../services/kafkaService');

// 创建新的同步任务
router.post('/', async (req, res) => {
    try {
        const { name, sourceConnection, targetConnection } = req.body;
        
        if (!name) {
            return res.status(400).json({
                success: false,
                message: '任务名称不能为空'
            });
        }
        
        // 创建工作流记录
        const workflow = await Workflow.create({
            name,
            sourceConnection: sourceConnection || 'default-source',
            targetConnection: targetConnection || 'default-target'
        });
        
        // 启动工作流执行
        workflowExecutor.startWorkflow(workflow.id);
        
        // 发送任务信息到 Kafka
        const taskInfo = {
            taskName: workflow.name,
            taskId: workflow.id,
            sourceConnection: workflow.sourceConnection,
            targetConnection: workflow.targetConnection,
            createTime: workflow.createdAt,
            executionProgress: workflow.progress || 0,
            currentStatus: workflow.status,
            updateTime: workflow.updatedAt
        };
        
        try {
            await kafkaService.sendMessage('sync-tasks', taskInfo);
            console.log('任务信息已发送到 Kafka');
        } catch (error) {
            console.error('发送 Kafka 消息失败:', error);
            // 不影响任务创建，继续执行
        }
        
        res.status(201).json({
            success: true,
            message: '同步任务创建成功',
            data: workflow
        });
    } catch (error) {
        console.error('创建任务失败:', error);
        res.status(500).json({
            success: false,
            message: '创建任务失败',
            error: error.message
        });
    }
});

// 获取所有任务列表
router.get('/', async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const pageSize = parseInt(req.query.pageSize) || 10;
        
        const result = await Workflow.findAll(page, pageSize);
        
        res.json({
            success: true,
            data: result
        });
    } catch (error) {
        console.error('获取任务列表失败:', error);
        res.status(500).json({
            success: false,
            message: '获取任务列表失败',
            error: error.message
        });
    }
});

// 获取单个任务详情
router.get('/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const workflow = await Workflow.findById(id);
        
        if (!workflow) {
            return res.status(404).json({
                success: false,
                message: '任务不存在'
            });
        }
        
        // 获取任务日志
        const logs = await Workflow.getLogs(id);
        
        res.json({
            success: true,
            data: {
                ...workflow,
                logs
            }
        });
    } catch (error) {
        console.error('获取任务详情失败:', error);
        res.status(500).json({
            success: false,
            message: '获取任务详情失败',
            error: error.message
        });
    }
});

// 暂停任务
router.post('/:id/pause', async (req, res) => {
    try {
        const { id } = req.params;
        
        await workflowExecutor.stopWorkflow(id);
        
        res.json({
            success: true,
            message: '任务已暂停'
        });
    } catch (error) {
        console.error('暂停任务失败:', error);
        res.status(500).json({
            success: false,
            message: '暂停任务失败',
            error: error.message
        });
    }
});

// 恢复任务
router.post('/:id/resume', async (req, res) => {
    try {
        const { id } = req.params;
        
        await workflowExecutor.startWorkflow(id);
        
        res.json({
            success: true,
            message: '任务已恢复'
        });
    } catch (error) {
        console.error('恢复任务失败:', error);
        res.status(500).json({
            success: false,
            message: '恢复任务失败',
            error: error.message
        });
    }
});

// 删除任务
router.delete('/:id', async (req, res) => {
    try {
        const { id } = req.params;
        
        // 先停止运行中的任务
        await workflowExecutor.stopWorkflow(id);
        
        // 删除任务
        const deleted = await Workflow.delete(id);
        
        if (!deleted) {
            return res.status(404).json({
                success: false,
                message: '任务不存在'
            });
        }
        
        res.json({
            success: true,
            message: '任务删除成功'
        });
    } catch (error) {
        console.error('删除任务失败:', error);
        res.status(500).json({
            success: false,
            message: '删除任务失败',
            error: error.message
        });
    }
});

module.exports = router;
