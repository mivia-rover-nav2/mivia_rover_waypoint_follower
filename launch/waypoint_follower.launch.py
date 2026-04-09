import os

from ament_index_python.packages import get_package_share_directory
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description():
    pkg_share = get_package_share_directory('mivia_rover_waypoint_follower')

    default_config = os.path.join(pkg_share, 'config', 'waypoint_follower.yaml')
    default_csv = os.path.join(pkg_share, 'waypoints', 'example_waypoints.csv')

    config_arg = DeclareLaunchArgument(
        'config',
        default_value=default_config,
        description='Path to the waypoint_follower YAML config file',
    )

    csv_arg = DeclareLaunchArgument(
        'waypoints_csv',
        default_value=default_csv,
        description='Absolute path to the CSV file with waypoints (x,y,qx,qy,qz,qw)',
    )

    waypoint_follower_node = Node(
        package='mivia_rover_waypoint_follower',
        executable='waypoint_follower',
        name='waypoint_follower',
        output='screen',
        parameters=[
            LaunchConfiguration('config'),
            {'waypoints_csv': LaunchConfiguration('waypoints_csv')},
        ],
    )

    return LaunchDescription([
        config_arg,
        csv_arg,
        waypoint_follower_node,
    ])
