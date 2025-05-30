import utils as u

def main(pcat, cfg, task, wildcards):
    cfg0 = u.dynamic_cfg(cfg,task,wildcards)
    out = pcat.search(**cfg0[task]["io"]["input"]).to_dataset(decode_time_delta=False)
    u.save_tmp_update_path(out, pcat, cfg=cfg0, task=task)

