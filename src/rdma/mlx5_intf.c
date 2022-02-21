
#include "mlx5_intf.h"


__attribute__((visibility ("hidden"))) 
void mlx5_build_ctrl_metadata(struct conn_context *ctx)
{
	uint8_t *tbl = ctx->fm_ce_se_tbl;
	uint8_t *acc = ctx->fm_ce_se_acc;
	int i;

	tbl[0		       | 0		   | 0]		     = (0			| 0			  | 0);
	tbl[0		       | 0		   | IBV_SEND_FENCE] = (0			| 0			  | MLX5_WQE_CTRL_FENCE);
	tbl[0		       | IBV_SEND_SIGNALED | 0]		     = (0			| MLX5_WQE_CTRL_CQ_UPDATE | 0);
	tbl[0		       | IBV_SEND_SIGNALED | IBV_SEND_FENCE] = (0			| MLX5_WQE_CTRL_CQ_UPDATE | MLX5_WQE_CTRL_FENCE);
	tbl[IBV_SEND_SOLICITED | 0		   | 0]		     = (MLX5_WQE_CTRL_SOLICITED | 0			  | 0);
	tbl[IBV_SEND_SOLICITED | 0		   | IBV_SEND_FENCE] = (MLX5_WQE_CTRL_SOLICITED | 0			  | MLX5_WQE_CTRL_FENCE);
	tbl[IBV_SEND_SOLICITED | IBV_SEND_SIGNALED | 0]		     = (MLX5_WQE_CTRL_SOLICITED | MLX5_WQE_CTRL_CQ_UPDATE | 0);
	tbl[IBV_SEND_SOLICITED | IBV_SEND_SIGNALED | IBV_SEND_FENCE] = (MLX5_WQE_CTRL_SOLICITED | MLX5_WQE_CTRL_CQ_UPDATE | MLX5_WQE_CTRL_FENCE);
	for (i = 0; i < 8; i++)
		tbl[i] = ctx->sq_signal_bits | tbl[i];

	memset(acc, 0, sizeof(ctx->fm_ce_se_acc));
	acc[0			       | 0			   | 0]			     = (0			| 0			  | 0);
	acc[0			       | 0			   | IBV_EXP_QP_BURST_FENCE] = (0			| 0			  | MLX5_WQE_CTRL_FENCE);
	acc[0			       | IBV_EXP_QP_BURST_SIGNALED | 0]			     = (0			| MLX5_WQE_CTRL_CQ_UPDATE | 0);
	acc[0			       | IBV_EXP_QP_BURST_SIGNALED | IBV_EXP_QP_BURST_FENCE] = (0			| MLX5_WQE_CTRL_CQ_UPDATE | MLX5_WQE_CTRL_FENCE);
	acc[IBV_EXP_QP_BURST_SOLICITED | 0			   | 0]			     = (MLX5_WQE_CTRL_SOLICITED | 0			  | 0);
	acc[IBV_EXP_QP_BURST_SOLICITED | 0			   | IBV_EXP_QP_BURST_FENCE] = (MLX5_WQE_CTRL_SOLICITED | 0			  | MLX5_WQE_CTRL_FENCE);
	acc[IBV_EXP_QP_BURST_SOLICITED | IBV_EXP_QP_BURST_SIGNALED | 0]			     = (MLX5_WQE_CTRL_SOLICITED | MLX5_WQE_CTRL_CQ_UPDATE | 0);
	acc[IBV_EXP_QP_BURST_SOLICITED | IBV_EXP_QP_BURST_SIGNALED | IBV_EXP_QP_BURST_FENCE] = (MLX5_WQE_CTRL_SOLICITED | MLX5_WQE_CTRL_CQ_UPDATE | MLX5_WQE_CTRL_FENCE);
	for (i = 0; i < 32; i++)
		acc[i] = ctx->sq_signal_bits | acc[i];
}

__attribute__((visibility ("hidden"))) 
int mlx5_post_send(struct ibv_exp_send_wr *wr, struct conn_context *ctx,
				   struct ibv_exp_send_wr **bad_wr, int is_exp_wr)
{
	void *seg;
	void *wqe2ring;
	int nreq;
	int err = 0;
	int size;
	unsigned idx;
	uint64_t exp_send_flags;

	for (nreq = 0; wr; ++nreq, wr = wr->next) {
		idx = ctx->scur_post & (ctx->iqp->sq.wqe_cnt - 1);
		seg = get_send_wqe(ctx, idx);

		exp_send_flags = is_exp_wr ? wr->exp_send_flags : ((struct ibv_send_wr *)wr)->send_flags;

		//XXX disable overrun checks
#if 0
		if (unlikely(!(qp->gen_data.create_flags & IBV_EXP_QP_CREATE_IGNORE_SQ_OVERFLOW) &&
			     mlx5_wq_overflow(0, nreq, qp))) {
			debug_print("work queue overflow\n");
			errno = ENOMEM;
			err = errno;
			*bad_wr = wr;
			goto out;
		}
#endif

		//XXX statically set SGE size
		if (wr->num_sge > 16) {
			debug_print("max gs exceeded %d (max = %d)\n",
				 wr->num_sge, 16);
			errno = ENOMEM;
			err = errno;
			*bad_wr = wr;
			goto out;
		}



		err = __mlx5_post_send(wr, ctx, exp_send_flags, seg, &size);
		if (err) {
			errno = err;
			*bad_wr = wr;
			goto out;
		}



		ctx->sq_wrid[idx] = wr->wr_id;

#if 0
		qp->gen_data.wqe_head[idx] = qp->sq.head + nreq;
#endif

		ctx->scur_post += DIV_ROUND_UP(size * 16, MLX5_SEND_WQE_BB);

		wqe2ring = seg;

	}

out:
	if (nreq) {

#if 1
		if (ctx->flags & IBV_EXP_QP_CREATE_MANAGED_SEND) {
			wmb();
			goto post_send_no_db;
		}
#else
		qp->sq.head += nreq;

		if (qp->gen_data.create_flags
					& CREATE_FLAG_NO_DOORBELL) {
			/* Controlled or peer-direct qp */
			wmb();
			if (qp->peer_enabled) {
				qp->peer_ctrl_seg = wqe2ring;
				qp->peer_seg_size += (size + 3) / 4;
			}
			goto post_send_no_db;
		}
#endif

		//XXX check which doorbell method to use. for now, putting MLX5_DB_METHOD_DB
		__ring_db(ctx, MLX5_DB_METHOD_DB, ctx->scur_post & 0xffff, wqe2ring, (size + 3) / 4);
	}

post_send_no_db:

	return err;
}

__attribute__((visibility ("hidden"))) 
int __mlx5_post_send(struct ibv_exp_send_wr *wr,
				      struct conn_context *ctx, uint64_t exp_send_flags, void *seg, int *total_size)
{
	struct mlx5_klm_buf *klm;
	void *ctrl = seg;
	struct ibv_qp *ibqp = ctx->id->qp;
	int err = 0;
	int size = 0;
	uint8_t opmod = 0;
	void *qend = ctx->sq_end;
	uint32_t mlx5_opcode;
	struct mlx5_wqe_xrc_seg *xrc;
	int tmp = 0;
	int num_sge = wr->num_sge;
	uint8_t next_fence = 0;
	struct mlx5_wqe_umr_ctrl_seg *umr_ctrl;
	int xlat_size;
	struct mlx5_mkey_seg *mk;
	int wqe_sz;
	uint64_t reglen;
	int atom_arg = 0;
	uint8_t fm_ce_se;
	uint32_t imm;

	//FILE *fp = to_mctx(qp->verbs_qp.qp.context)->dbg_fp;

#if 0
	if (unlikely(((MLX5_IB_OPCODE_GET_CLASS(mlx5_ib_opcode[wr->exp_opcode]) == MLX5_OPCODE_MANAGED) ||
		      (exp_send_flags & IBV_EXP_SEND_WITH_CALC)) &&
		     !(qp->gen_data.create_flags & IBV_EXP_QP_CREATE_CROSS_CHANNEL))) {
		mlx5_dbg(fp, MLX5_DBG_QP_SEND, "unsupported cross-channel functionality\n");
		return EINVAL;
	}
#endif

	mlx5_opcode = MLX5_WRAPPER_IB_OPCODE_GET_OP(mlx5_ib_opcode[wr->exp_opcode]);
	imm = send_ieth(wr);

	seg += sizeof(struct mlx5_wqe_ctrl_seg);
	size = sizeof(struct mlx5_wqe_ctrl_seg) / 16;

	switch (wr->exp_opcode) {
	case IBV_EXP_WR_RDMA_READ:
	case IBV_EXP_WR_RDMA_WRITE:
	case IBV_EXP_WR_RDMA_WRITE_WITH_IMM:
		//XXX disabling validity checks for now
#if 0
		if (unlikely(exp_send_flags & IBV_EXP_SEND_WITH_CALC)) {

			if ((uint32_t)wr->op.calc.data_size >= IBV_EXP_CALC_DATA_SIZE_NUMBER ||
			    (uint32_t)wr->op.calc.calc_op >= IBV_EXP_CALC_OP_NUMBER ||
			    (uint32_t)wr->op.calc.data_type >= IBV_EXP_CALC_DATA_TYPE_NUMBER ||
			    !mlx5_calc_ops_table[wr->op.calc.data_size][wr->op.calc.calc_op]
						[wr->op.calc.data_type].valid)
				return EINVAL;

			opmod = mlx5_calc_ops_table[wr->op.calc.data_size][wr->op.calc.calc_op]
							  [wr->op.calc.data_type].opmod;

		}
#endif
		set_raddr_seg(seg, wr->wr.rdma.remote_addr, wr->wr.rdma.rkey);
		seg  += sizeof(struct mlx5_wqe_raddr_seg);
		size += sizeof(struct mlx5_wqe_raddr_seg) / 16;
		break;

	case IBV_EXP_WR_ATOMIC_CMP_AND_SWP:
	case IBV_EXP_WR_ATOMIC_FETCH_AND_ADD:
#if 0
		if (unlikely(!qp->enable_atomics)) {
			mlx5_dbg(fp, MLX5_DBG_QP_SEND, "atomics not allowed\n");
			return EINVAL;
		}
#endif
		set_raddr_seg(seg, wr->wr.atomic.remote_addr,
			      wr->wr.atomic.rkey);
		seg  += sizeof(struct mlx5_wqe_raddr_seg);

		set_atomic_seg(seg, wr->exp_opcode, wr->wr.atomic.swap,
			       wr->wr.atomic.compare_add);
		seg  += sizeof(struct mlx5_wqe_atomic_seg);

		size += (sizeof(struct mlx5_wqe_raddr_seg) +
		sizeof(struct mlx5_wqe_atomic_seg)) / 16;
		atom_arg = 8;
		break;
	case IBV_EXP_WR_SEND:
		//XXX disabling validity and opmod checks
#if 0
		if (unlikely(exp_send_flags & IBV_EXP_SEND_WITH_CALC)) {

			if ((uint32_t)wr->op.calc.data_size >= IBV_EXP_CALC_DATA_SIZE_NUMBER ||
			    (uint32_t)wr->op.calc.calc_op >= IBV_EXP_CALC_OP_NUMBER ||
			    (uint32_t)wr->op.calc.data_type >= IBV_EXP_CALC_DATA_TYPE_NUMBER ||
			    !mlx5_calc_ops_table[wr->op.calc.data_size][wr->op.calc.calc_op]
						[wr->op.calc.data_type].valid)
				return EINVAL;

			opmod = mlx5_calc_ops_table[wr->op.calc.data_size][wr->op.calc.calc_op]
							  [wr->op.calc.data_type].opmod;
		}
#endif
		break;

	case IBV_EXP_WR_CQE_WAIT:
		{
#if 1
			// set wait index to value provided by usr
			uint32_t wait_index = wr->task.cqe_wait.cq_count;
#else
			uint32_t wait_index = 0;
			struct mlx5_cq *wait_cq = to_mcq(wr->task.cqe_wait.cq);

			wait_index = wait_cq->wait_index +
					wr->task.cqe_wait.cq_count;
			wait_cq->wait_count = max(wait_cq->wait_count,
					wr->task.cqe_wait.cq_count);

			if (exp_send_flags & IBV_EXP_SEND_WAIT_EN_LAST) {
				wait_cq->wait_index += wait_cq->wait_count;
				wait_cq->wait_count = 0;
			}
#endif

			set_wait_en_seg(seg, ctx->icq->cqn, wait_index);
			seg   += sizeof(struct mlx5_wqe_wait_en_seg);
			size += sizeof(struct mlx5_wqe_wait_en_seg) / 16;
		}
		break;

	case IBV_EXP_WR_SEND_ENABLE:
	case IBV_EXP_WR_RECV_ENABLE:
		{
			unsigned head_en_index;
#if 1
			/*
			 * Posting work request for QP that does not support
			 * SEND/RECV ENABLE makes performance worse.
			 */
			//XXX should check target queue instead
			if (((wr->exp_opcode == IBV_EXP_WR_SEND_ENABLE) &&
				!(ctx->flags & IBV_EXP_QP_CREATE_MANAGED_SEND)) ||
				((wr->exp_opcode == IBV_EXP_WR_RECV_ENABLE) &&
				!(ctx->flags & IBV_EXP_QP_CREATE_MANAGED_RECV))) {
				return EINVAL;
			}

#else
			struct mlx5_wq *wq;
			struct mlx5_wq_recv_send_enable *wq_en;


			/*
			 * Posting work request for QP that does not support
			 * SEND/RECV ENABLE makes performance worse.
			 */
			if (((wr->exp_opcode == IBV_EXP_WR_SEND_ENABLE) &&
				!(to_mqp(wr->task.wqe_enable.qp)->gen_data.create_flags &
					IBV_EXP_QP_CREATE_MANAGED_SEND)) ||
				((wr->exp_opcode == IBV_EXP_WR_RECV_ENABLE) &&
				!(to_mqp(wr->task.wqe_enable.qp)->gen_data.create_flags &
					IBV_EXP_QP_CREATE_MANAGED_RECV))) {
				return EINVAL;
			}
#endif

#if 1
			// set enable index to value provided by usr
			head_en_index = wr->task.wqe_enable.wqe_count;
#else
			wq = (wr->exp_opcode == IBV_EXP_WR_SEND_ENABLE) ?
				&to_mqp(wr->task.wqe_enable.qp)->sq :
				&to_mqp(wr->task.wqe_enable.qp)->rq;

			wq_en = (wr->exp_opcode == IBV_EXP_WR_SEND_ENABLE) ?
				 &to_mqp(wr->task.wqe_enable.qp)->sq_enable :
				 &to_mqp(wr->task.wqe_enable.qp)->rq_enable;

			/* If wqe_count is 0 release all WRs from queue */
			if (wr->task.wqe_enable.wqe_count) {
				head_en_index = wq_en->head_en_index +
							wr->task.wqe_enable.wqe_count;
				wq_en->head_en_count = max(wq_en->head_en_count,
							   wr->task.wqe_enable.wqe_count);

				if ((int)(wq->head - head_en_index) < 0)
					return EINVAL;
			} else {
				head_en_index = wq->head;
				wq_en->head_en_count = wq->head - wq_en->head_en_index;
			}

			if (exp_send_flags & IBV_EXP_SEND_WAIT_EN_LAST) {
				wq_en->head_en_index += wq_en->head_en_count;
				wq_en->head_en_count = 0;
			}
#endif
			set_wait_en_seg(seg,
					wr->task.wqe_enable.qp->qp_num,
					head_en_index);

			seg += sizeof(struct mlx5_wqe_wait_en_seg);
			size += sizeof(struct mlx5_wqe_wait_en_seg) / 16;
		}
		break;
	
	case IBV_EXP_WR_NOP:
		break;
	default:
		break;
	}


	err = set_data_seg(ctx, seg, &size, !!(exp_send_flags & IBV_EXP_SEND_INLINE),
			   num_sge, wr->sg_list, atom_arg, 0, 0);
	if (err)
		return err;

	fm_ce_se = ctx->fm_ce_se_tbl[exp_send_flags & (IBV_SEND_SOLICITED | IBV_SEND_SIGNALED | IBV_SEND_FENCE)];
	//fm_ce_se |= get_fence(qp->gen_data.fm_cache, wr);


	//XXX find value of scur_post
	uint16_t scur_post = 0;
	set_ctrl_seg_sig(ctrl, ctx->id->qp->qp_num,
			 mlx5_opcode, scur_post, opmod, size,
			 fm_ce_se, imm);
	
	//qp->gen_data.fm_cache = next_fence;
	*total_size = size;

	return 0;
}

int ibv_post_send_wrapper(struct conn_context *ctx, struct ibv_qp *qp, struct ibv_send_wr *wr,
		                         struct ibv_send_wr **bad_wr)
{
#ifndef MODDED_DRIVER
	update_scur_post((struct ibv_exp_send_wr *)wr, ctx);
#endif
	return ibv_post_send(qp, wr, bad_wr);
}

int ibv_exp_post_send_wrapper(struct conn_context *ctx, struct ibv_qp *qp, struct ibv_exp_send_wr *wr,
		struct ibv_exp_send_wr **bad_wr)
{
#ifndef MODDED_DRIVER
	update_scur_post(wr, ctx);
#endif
	return ibv_exp_post_send(qp, wr, bad_wr);
}

__attribute__((visibility ("hidden"))) 
int update_scur_post(struct ibv_exp_send_wr *wr, struct conn_context *ctx)
{

#ifdef IBV_WRAPPER_INLINE
	printf("inlining not supported\n");
	exit(EXIT_FAILURE);
#endif

	uint32_t idx = ctx->scur_post & (ctx->iqp->sq.wqe_cnt - 1);
	
	ctx->sq_wrid[idx] = wr->wr_id;

	int size = sizeof(struct mlx5_wqe_ctrl_seg) / 16;

	switch (wr->exp_opcode) {
	case IBV_EXP_WR_RDMA_READ:
	case IBV_EXP_WR_RDMA_WRITE:
	case IBV_EXP_WR_RDMA_WRITE_WITH_IMM:
		//XXX disabling validity checks for now
#if 0
		if (unlikely(exp_send_flags & IBV_EXP_SEND_WITH_CALC)) {

			if ((uint32_t)wr->op.calc.data_size >= IBV_EXP_CALC_DATA_SIZE_NUMBER ||
			    (uint32_t)wr->op.calc.calc_op >= IBV_EXP_CALC_OP_NUMBER ||
			    (uint32_t)wr->op.calc.data_type >= IBV_EXP_CALC_DATA_TYPE_NUMBER ||
			    !mlx5_calc_ops_table[wr->op.calc.data_size][wr->op.calc.calc_op]
						[wr->op.calc.data_type].valid)
				return EINVAL;

			opmod = mlx5_calc_ops_table[wr->op.calc.data_size][wr->op.calc.calc_op]
							  [wr->op.calc.data_type].opmod;

		}
#endif
		size += sizeof(struct mlx5_wqe_raddr_seg) / 16;
		break;

	case IBV_EXP_WR_ATOMIC_CMP_AND_SWP:
	case IBV_EXP_WR_ATOMIC_FETCH_AND_ADD:
#if 0
		if (unlikely(!qp->enable_atomics)) {
			mlx5_dbg(fp, MLX5_DBG_QP_SEND, "atomics not allowed\n");
			return EINVAL;
		}
#endif
		size += (sizeof(struct mlx5_wqe_raddr_seg) + sizeof(struct mlx5_wqe_atomic_seg)) / 16;
		break;
	case IBV_EXP_WR_SEND:
		//XXX disabling validity and opmod checks
#if 0
		if (unlikely(exp_send_flags & IBV_EXP_SEND_WITH_CALC)) {

			if ((uint32_t)wr->op.calc.data_size >= IBV_EXP_CALC_DATA_SIZE_NUMBER ||
			    (uint32_t)wr->op.calc.calc_op >= IBV_EXP_CALC_OP_NUMBER ||
			    (uint32_t)wr->op.calc.data_type >= IBV_EXP_CALC_DATA_TYPE_NUMBER ||
			    !mlx5_calc_ops_table[wr->op.calc.data_size][wr->op.calc.calc_op]
						[wr->op.calc.data_type].valid)
				return EINVAL;

			opmod = mlx5_calc_ops_table[wr->op.calc.data_size][wr->op.calc.calc_op]
							  [wr->op.calc.data_type].opmod;
		}
#endif
		break;

	case IBV_EXP_WR_CQE_WAIT:
		{
			size += sizeof(struct mlx5_wqe_wait_en_seg) / 16;
		}
		break;

	case IBV_EXP_WR_SEND_ENABLE:
	case IBV_EXP_WR_RECV_ENABLE:
		{
			size += sizeof(struct mlx5_wqe_wait_en_seg) / 16;
		}
		break;
	
	case IBV_EXP_WR_NOP:
		break;
	default:
		break;
	}

	for(int i=0; i<wr->num_sge; i++) {
		if (wr->sg_list[i].length > 0) {
			size += sizeof(struct mlx5_wqe_data_seg) / 16;
		}
	}

	printf("updating scur_post %u by %d (original size %d)\n", ctx->scur_post, DIV_ROUND_UP(size * 16, MLX5_SEND_WQE_BB), size);
	ctx->scur_post += DIV_ROUND_UP(size * 16, MLX5_SEND_WQE_BB);

	return 0;
}


