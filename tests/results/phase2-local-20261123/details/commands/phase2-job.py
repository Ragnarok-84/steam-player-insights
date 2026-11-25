import argparse
import json


NAMESPACE = 'steam-demo'
IMAGE = 'localhost/steam-spark:k3s-20261120-af8377d'
HDFS = 'hdfs://namenode:8020/steam'
ES = 'http://elasticsearch:9200'


def make_job(stage, starting_offsets, mode, name=None):
    env = {
        'PIPELINE_PROFILE': 'cluster',
        'HDFS_NAMENODE_URL': 'hdfs://namenode:8020',
        'HDFS_RAW_PATH': f'{HDFS}/raw',
        'HDFS_CLEAN_PATH': f'{HDFS}/clean',
        'HDFS_CURATED_PATH': f'{HDFS}/curated',
        'HDFS_CHECKPOINT_PATH': f'{HDFS}/checkpoints',
        'HADOOP_USER_NAME': 'hdfs',
        'KAFKA_BOOTSTRAP_SERVERS': 'kafka:9092',
        'ELASTICSEARCH_HOSTS': ES,
        'PYTHONUNBUFFERED': '1',
        'SPARK_SHUFFLE_PARTITIONS': '6',
        'SPARK_DRIVER_MEMORY': '2g',
    }
    conf = {'spark.hadoop.dfs.client.use.datanode.hostname': 'true', 'spark.hadoop.dfs.replication': '1'}
    driver = 'steam-worker-2'
    distributed = stage in ('b1', 'batch') and mode == 'distributed'
    fields = []
    if distributed:
        env['SPARK_MASTER'] = 'k8s://https://kubernetes.default.svc:443'
        env['SPARK_EXTRA_CONF'] = (
            'spark.kubernetes.driver.pod.name=$(PHASE2_POD_NAME);'
            'spark.kubernetes.namespace=steam-demo;'
            f'spark.kubernetes.container.image={IMAGE};'
            'spark.kubernetes.container.image.pullPolicy=Never;'
            'spark.kubernetes.node.selector.kubernetes.io/hostname=steam-worker-3;'
            'spark.executor.instances=1;spark.executor.cores=1;spark.executor.memory=2g;'
            'spark.executor.memoryOverhead=1024m;'
            'spark.kubernetes.executor.request.cores=1;spark.kubernetes.executor.limit.cores=1;'
            'spark.driver.bindAddress=0.0.0.0;spark.driver.port=7078;spark.blockManager.port=7079;'
            f'spark.executorEnv.ELASTICSEARCH_HOSTS={ES};spark.executorEnv.HADOOP_USER_NAME=hdfs;'
            'spark.dynamicAllocation.enabled=false;'
        )
        for variable, field in [('PHASE2_POD_NAME', 'metadata.name'), ('SPARK_DRIVER_HOST', 'status.podIP')]:
            fields.append({'name': variable, 'valueFrom': {'fieldRef': {'fieldPath': field}}})
    else:
        env['SPARK_MASTER'] = 'local[2]'
    env['SPARK_EXTRA_CONF'] = env.get('SPARK_EXTRA_CONF', '') + ';'.join(f'{k}={v}' for k, v in conf.items())

    command = ['python3', 'pipeline.py', '--profile', 'cluster', stage]
    if stage == 'batch':
        command.append('--es')
    if stage == 'inject':
        command = ['python3', '/opt/phase2/phase2-stream.py', 'inject']
    if stage in ('s1', 's2', 's3'):
        if not starting_offsets:
            raise ValueError('Streaming requires the exact pre-injection Kafka offset')
        json.loads(starting_offsets)
        command = ['python3', '/opt/phase2/phase2-stream.py', stage,
                   '--starting-offsets', starting_offsets,
                   '--checkpoint', f'{HDFS}/checkpoints/phase2-{stage}']
    if stage == 'kibana-setup':
        env['KIBANA_HOST'] = 'http://kibana:5601'
        command = ['python3', '-m', 'dashboards.setup_kibana']

    resources = {'requests': {'cpu': '1', 'memory': '3Gi'}, 'limits': {'cpu': '2', 'memory': '4300Mi'}}
    container = {'name': stage, 'image': IMAGE, 'imagePullPolicy': 'Never', 'command': command,
                 'env': fields + [{'name': k, 'value': v} for k, v in env.items()], 'resources': resources}
    pod = {'restartPolicy': 'Never', 'nodeSelector': {'kubernetes.io/hostname': driver}, 'containers': [container],
           'automountServiceAccountToken': distributed}
    if distributed:
        pod['serviceAccountName'] = 'spark-operator-sa'
    if stage in ('inject', 's1', 's2', 's3'):
        pod['volumes'] = [{'name': 'phase2-scripts', 'configMap': {'name': 'phase2-scripts'}}]
        container['volumeMounts'] = [{'name': 'phase2-scripts', 'mountPath': '/opt/phase2', 'readOnly': True}]
    return {'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': {'namespace': NAMESPACE, 'name': name or f'phase2-{stage}'},
            'spec': {'backoffLimit': 0, 'activeDeadlineSeconds': 3600, 'template': {'spec': pod}}}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', choices=['b1', 'batch', 'inject', 's1', 's2', 's3', 'kibana-setup'])
    parser.add_argument('--starting-offsets')
    parser.add_argument('--mode', default='distributed', choices=['distributed', 'local'])
    parser.add_argument('--name')
    args = parser.parse_args()
    print(json.dumps(make_job(args.stage, args.starting_offsets, args.mode, args.name)))
