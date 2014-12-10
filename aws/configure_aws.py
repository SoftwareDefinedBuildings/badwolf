import boto.ec2
from configobj import ConfigObj
import time

# configuration vars
config = ConfigObj('aws.ini')['aws']
print config
AWS_SECRET_ACCESS_KEY=config['AWS_SECRET_ACCESS_KEY']
AWS_ACCESS_KEY=config['AWS_ACCESS_KEY']
image_id = config['image_id']
key_name = config['key_name']
instance_type = config['instance_type']
security_group_ids = config['security_group_ids']
if not isinstance(security_group_ids, list):
    security_group_ids = [security_group_ids]
subnet_id = config['subnet_id']
vpc_id = config['vpc_id']
availability_zone = config['availability_zone']
region = config['region']

# we use this connection to create instances
conn = boto.ec2.connect_to_region(region,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

def create_some_spots(num, maxbid=None):
    """
    Create [num] spot instances. If maxbid ($) is specified, uses the maximum bid, else adds 10 cents to the most recent
    price
    """
    if maxbid is None:
        history = conn.get_spot_price_history(instance_type = instance_type, availability_zone=availability_zone, product_description = "Linux/UNIX (Amazon VPC)")
        maxbid = history[0].price + .1
    interface = boto.ec2.networkinterface.NetworkInterfaceSpecification(subnet_id=subnet_id,
                                                                    groups=security_group_ids,
                                                                    associate_public_ip_address=True)
    interfaces = boto.ec2.networkinterface.NetworkInterfaceCollection(interface)
    instances = conn.request_spot_instances(str(maxbid), image_id,
                                            count = num,
                                            availability_zone_group = availability_zone,
                                            key_name = key_name,
                                            instance_type = instance_type,
                                            network_interfaces=interfaces,
                                            dry_run = False)

    state = 'open'
    check_index = 0
    while state == 'open':
        time.sleep(10)
        spot = conn.get_all_spot_instance_requests(instances[check_index].id)[0]
        state = spot.state
        if state == 'active':
            check_index = min(check_index+1, num)
            if check_index == num: break
            state = 'open'
        print 'Still checking spot request id', instances[check_index].id
    spot_instance_ids = map(lambda x: x.id, instances)
    return conn.get_all_spot_instance_requests(spot_instance_ids)

def get_ips(spot_instances):
    instance_ids = map(lambda x: x.instance_id, spot_instances)
    instances = conn.get_only_instances(instance_ids=instance_ids)
    return map(lambda x: (x.private_ip_address, x.ip_address), instances)

def stop_instances(spot_instances):
    # just pass this the output of create_some_spots and it will terminate just those instances
    # returns a list of terminated instances
    instance_ids = map(lambda x: x.instance_id, spot_instances)
    return conn.terminate_instances(instance_ids=instance_ids)



if __name__ == '__main__':
    import sys
    num = int(sys.argv[1])
    try:
        spot_instances = create_some_spots(num)
        ips = get_ips(spot_instances)
        with open('ips.csv','w+') as f:
            for ip in ips:
                f.write(ip+'\n')
    except Exception as e:
        print "got exception",e,"now dropping into console"
        import IPython
        IPython.embed(user_ns=locals())
