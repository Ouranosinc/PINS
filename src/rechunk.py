import utils as u 
def rechunk(ds, cfg, task):
    return ds.chunk(cfg[task]["chunks"])

def main(pcat, cfg, task, wildcards): 
    dd, cfg0 = u.dynamic_io(pcat,cfg, task, wildcards)
    out = rechunk(dd["input"], cfg0, task)
    u.save_tmp_update_path(out, pcat, cfg=cfg0, task=task)
