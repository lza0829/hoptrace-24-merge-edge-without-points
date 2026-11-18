# 途迹（Hoptrace）

轨迹点装载 → 运单时间线物化 → 迟到旁路与脏点隔离 → 状态证明互证。

**不是** Feed 归并、CDC 游标 Sink、质检签发、数仓分层、交班过户、正冲净额。  
**也不是** 电商订单 / 仓库库存 / 外卖工单 CRUD——去皮后是事件装载与有序物化。

## 启动

```bash
# 后端
cd backend
pip install -r requirements.txt
set DATA_DIR=%CD%\data
set FIXTURES_DIR=%CD%\..\fixtures
uvicorn app.main:app --host 127.0.0.1 --port 10800

# 前端
cd frontend
npm install
npm run dev
```

| 入口 | 地址 |
| --- | --- |
| 前端 | http://127.0.0.1:5800 |
| 接口 | http://127.0.0.1:10800 |

## UI

三栏测绘板：顶品牌条 + 中竖轴时间线 + 右检查器 + 底 hop 点阵。  
不使用左轨编号导航、不使用顶栏流程 ribbon。

## 多段并单（merge_leg_proof）

成员段正式有效点按 `(event_ts, id)` 并入主单正式时间线；合并后成员段冻结，
装载新点不再独立漂移主单投影。

- **落库形态**：独立合并边表 `merge_edges`（成员唯一）+ 并入点溯源 `merge_hops`
  + 批次证明 `merge_batches`。不做「只改主单状态字段」冒充合并。
- **一致性路径**：单事务内完成「并入点 + 写边 + 标记成员 + 重算投影」，
  任一步异常整笔回滚；失败尝试留一条 `rolled_back` 批次供审计，
  但边 / 并入点 / 投影零残留。
- **证明接口**（只读）：`GET /api/merge/batches/{batch_id}` 返回主单、成员列表、
  并入点数、并入点清单、前后投影；`GET /api/merge/batches` 列批次。
  测绘板 / 并单页 / 证明页的数字都回查同一批次证明。

| 动作 | 接口 |
| --- | --- |
| 跑成功合并夹具 | `POST /api/merge/fixture?mode=success` |
| 跑中途失败夹具（回滚对照） | `POST /api/merge/fixture?mode=failure` |
| 指定主单与成员合并 | `POST /api/merge/run` `{master_id, member_ids}` |
| 批次证明 / 批次列表 | `GET /api/merge/batches/{id}` · `GET /api/merge/batches` |

夹具：`fixtures/merge_batch.json`（主单 + 两段成员，共 4 个可并入点）。

```bash
cd backend
pytest tests -q
```
