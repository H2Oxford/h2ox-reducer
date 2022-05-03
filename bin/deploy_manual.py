import yaml

from h2ox.reducer.gcp_utils import create_task, deploy_task

if __name__=="__main__":
    
    deploy_day = '2022-05-03'
    
    cfg = yaml.load(open('./queue.yaml','r'), Loader=yaml.SafeLoader)

    task = create_task(
        cfg=cfg,
        payload=dict(today=deploy_day),
        task_name=deploy_day,
        delay=0,
    )

    deploy_task(cfg, task)
