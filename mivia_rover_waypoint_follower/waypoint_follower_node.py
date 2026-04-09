import csv
import os

import rclpy
from rclpy.action import ActionClient
from rclpy.node import Node

from geometry_msgs.msg import PoseStamped
from nav2_msgs.action import FollowPath
from nav_msgs.msg import Path


class WaypointFollowerNode(Node):

    def __init__(self):
        super().__init__('waypoint_follower')

        self.declare_parameter('waypoints_csv', '')
        self.declare_parameter('controller_id', 'FollowPath')
        self.declare_parameter('goal_checker_id', 'general_goal_checker')

        csv_path = self.get_parameter('waypoints_csv').get_parameter_value().string_value
        self._controller_id = self.get_parameter('controller_id').get_parameter_value().string_value
        self._goal_checker_id = self.get_parameter('goal_checker_id').get_parameter_value().string_value

        if not csv_path:
            self.get_logger().fatal('Parameter "waypoints_csv" is not set. Shutting down.')
            raise SystemExit(1)

        if not os.path.isfile(csv_path):
            self.get_logger().fatal(f'CSV file not found: {csv_path}')
            raise SystemExit(1)

        self._waypoints = self._load_csv(csv_path)

        if not self._waypoints:
            self.get_logger().fatal('No valid waypoints loaded from CSV. Shutting down.')
            raise SystemExit(1)

        self._client = ActionClient(self, FollowPath, '/follow_path')
        self._goal_handle = None
        self._started = False

        # Defer path sending to allow NAV2 lifecycle to complete
        self.create_timer(1.0, self._start)

    def _load_csv(self, path: str) -> list:
        waypoints = []
        with open(path, newline='') as f:
            reader = csv.reader(f)
            for row in reader:
                # Skip empty lines and comment lines
                if not row or row[0].strip().startswith('#'):
                    continue
                if any(v.strip().lstrip('-').replace('.', '', 1).isalpha() for v in row):
                    # Row contains letters: it's a header or malformed — skip
                    continue
                if len(row) < 6:
                    self.get_logger().warn(
                        f'Expected 6 values (x,y,qx,qy,qz,qw), got {len(row)} — skipped: {row}'
                    )
                    continue
                try:
                    x, y, qx, qy, qz, qw = [float(v.strip()) for v in row[:6]]
                    waypoints.append((x, y, qx, qy, qz, qw))
                except ValueError as e:
                    self.get_logger().warn(f'Could not parse row {row}: {e} — skipped')

        self.get_logger().info(f'Loaded {len(waypoints)} waypoints from: {path}')
        return waypoints

    def _build_path(self) -> Path:
        path = Path()
        path.header.frame_id = 'map'
        path.header.stamp = self.get_clock().now().to_msg()

        for x, y, qx, qy, qz, qw in self._waypoints:
            pose = PoseStamped()
            pose.header = path.header
            pose.pose.position.x = x
            pose.pose.position.y = y
            pose.pose.orientation.x = qx
            pose.pose.orientation.y = qy
            pose.pose.orientation.z = qz
            pose.pose.orientation.w = qw
            path.poses.append(pose)

        return path

    def _start(self):
        if self._started:
            return
        self._started = True

        self.get_logger().info('Waiting for /follow_path action server...')
        self._client.wait_for_server()
        self._send_path()

    def _send_path(self):
        path = self._build_path()

        goal = FollowPath.Goal()
        goal.path = path
        goal.controller_id = self._controller_id
        goal.goal_checker_id = self._goal_checker_id

        self.get_logger().info(
            f'Sending path with {len(path.poses)} poses '
            f'[controller: {self._controller_id}]'
        )

        send_future = self._client.send_goal_async(
            goal,
            feedback_callback=self._feedback_cb
        )
        send_future.add_done_callback(self._goal_response_cb)

    def _goal_response_cb(self, future):
        self._goal_handle = future.result()
        if not self._goal_handle.accepted:
            self.get_logger().error('Path rejected by controller server.')
            return

        self.get_logger().info('Path accepted. Following...')
        result_future = self._goal_handle.get_result_async()
        result_future.add_done_callback(self._result_cb)

    def _feedback_cb(self, feedback_msg):
        pass

    def _result_cb(self, future):
        status = future.result().status
        if status == 4:  # SUCCEEDED
            self.get_logger().info('Path following completed successfully.')
        else:
            self.get_logger().warn(f'Path following ended with status: {status}')


def main(args=None):
    rclpy.init(args=args)
    node = WaypointFollowerNode()
    rclpy.spin(node)
    rclpy.shutdown()
