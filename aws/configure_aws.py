import boto.ec2
from configobj import ConfigObj
import time

# configuration vars
config = ConfigObj('aws.ini')['aws']
print config
AWS_SECRET_ACCESS_KEY=config['AWS_SECRET_ACCESS_KEY']
AWS_ACCESS_KEY=config['AWS_ACCESS_KEY']
smap_ami = config['image_id']
key_name = config['key_name']
instance_type = config['instance_type']
security_group_ids = config['security_group_ids']
if not isinstance(security_group_ids, list):
    security_group_ids = [security_group_ids]
subnet_id = config['subnet_id']
region = config['region']

# we use this connection to create instances
conn = boto.ec2.connect_to_region(region,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

def get_instance(conn, ami, key, instance, security_groups, subnet):
    """
    Create an instance on EC2 with the given parameters
    """
    reservation = conn.run_instances(ami, key_name=key, instance_type=instance, security_group_ids=security_groups, subnet_id=subnet_id)
    instance = reservation.instances[0]
    instance.update()
    while instance.state == 'pending':
        print instance,instance.state,instance.ip_address
        time.sleep(5)
        instance.update()
    print 'GOT ip', instance, instance.state, instance.ip_address
    return instance

def create_some(num):
    """
    Create [num] instances and return the list of created IP addresses
    """
    ips = []
    for i in range(num):
        instance = get_instance(conn, smap_ami, key_name, instance_type, security_group_ids, subnet_id)
        ips.append(instance.ip_address)
    return ips

if __name__ == '__main__':
    import sys
    num = int(sys.argv[1])
    ips = create_some(num)
    with open('ips.csv','w+') as f:
        for ip in ips:
            f.write(ip+'\n')
