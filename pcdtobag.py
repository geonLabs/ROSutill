import os
import pcl
import rospy
import rosbag
from sensor_msgs.msg import PointCloud2, PointField
import sensor_msgs.point_cloud2 as pc2
import std_msgs.msg

def pcd_to_pointcloud2(pcd_file, seq, timestamp):
    # PCD 파일 로드
    cloud = pcl.load_XYZI(pcd_file)
    
    if cloud.size == 0:
        raise ValueError(f"No points found in PCD file: {pcd_file}")

    # 헤더 생성
    header = std_msgs.msg.Header()
    header.stamp = timestamp
    header.frame_id = 'map'
    header.seq = seq

    # 필드 정의
    fields = [
        PointField('x', 0, PointField.FLOAT32, 1),
        PointField('y', 4, PointField.FLOAT32, 1),
        PointField('z', 8, PointField.FLOAT32, 1),
        PointField('intensity', 12, PointField.FLOAT32, 1),
    ]
    
    # 포인트 데이터 생성
    point_data = []
    for point in cloud:
        point_data.append([point[0], point[1], point[2], point[3]])

    # 포인트 클라우드 메시지 생성
    point_cloud_msg = pc2.create_cloud(header, fields, point_data)
    return point_cloud_msg

def create_bag_from_pcd_folder(pcd_folder, bag_file):
    # ROS 초기화
    rospy.init_node('pcd_to_bag', anonymous=True)

    # BAG 파일 생성
    bag = rosbag.Bag(bag_file, 'w')

    try:
        seq = 0
        timestamp = rospy.Time.now()
        for pcd_file in sorted(os.listdir(pcd_folder)):
            if pcd_file.endswith('.pcd'):
                full_path = os.path.join(pcd_folder, pcd_file)
                try:
                    point_cloud_msg = pcd_to_pointcloud2(full_path, seq, timestamp)
                    bag.write('/point_cloud', point_cloud_msg)
                    seq += 1
                    timestamp += rospy.Duration(0.1)  # 10Hz 간격으로 타임스탬프 증가
                except ValueError as e:
                    rospy.logwarn(e)
    finally:
        bag.close()

if __name__ == "__main__":
    pcd_folder = '/mnt/d/map_merge/pcd/01'  # 입력 PCD 파일들이 있는 폴더 경로
    bag_file = 'output.bag'    # 출력 BAG 파일 경로
    create_bag_from_pcd_folder(pcd_folder, bag_file)

