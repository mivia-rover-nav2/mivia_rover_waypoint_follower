import csv
import math
import os

import rclpy
from action_msgs.msg import GoalStatus
from geometry_msgs.msg import PoseStamped
from nav2_msgs.action import FollowPath
from nav_msgs.msg import Odometry, Path
from rclpy.action import ActionClient
from rclpy.node import Node


class WaypointFollowerNode(Node):

    def __init__(self):
        super().__init__('waypoint_follower')

        self.declare_parameter('waypoints_csv', '')
        self.declare_parameter('controller_id', 'FollowPath')
        self.declare_parameter('goal_checker_id', 'general_goal_checker')
        self.declare_parameter('odom_topic', '/odometry/filtered')
        self.declare_parameter('circular_threshold', 0.5)
        self.declare_parameter('circular_loops_ahead', 2.5)

        csv_path = self.get_parameter('waypoints_csv').get_parameter_value().string_value
        self._controller_id = self.get_parameter('controller_id').get_parameter_value().string_value
        self._goal_checker_id = self.get_parameter('goal_checker_id').get_parameter_value().string_value
        self._odom_topic = self.get_parameter('odom_topic').get_parameter_value().string_value
        self._circular_threshold = self.get_parameter('circular_threshold').get_parameter_value().double_value
        self._circular_loops_ahead = max(
            0.1,
            self.get_parameter('circular_loops_ahead').get_parameter_value().double_value,
        )

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

        self._is_circular = self._detect_circular()
        if self._is_circular:
            self._waypoints = self._prepare_circular_waypoints(self._waypoints)
            if len(self._waypoints) < 2:
                self.get_logger().fatal(
                    'Circular trajectory requires at least 2 distinct waypoints. Shutting down.'
                )
                raise SystemExit(1)

        self._loop_length = self._compute_path_length(self._waypoints, wrap=self._is_circular)
        if self._is_circular and self._loop_length <= 0.0:
            self.get_logger().fatal(
                'Circular trajectory has zero length after preprocessing. Shutting down.'
            )
            raise SystemExit(1)

        self._current_position = None
        self._odom_sub = self.create_subscription(Odometry, self._odom_topic, self._odom_cb, 10)

        self._client = ActionClient(self, FollowPath, '/follow_path')
        self._goal_handle = None
        self._started = False
        self._send_in_progress = False
        self._request_id = 0
        self._active_request_id = None
        self._last_nearest_index = None
        self._circular_progress_points = 0
        self._refresh_after_points = 0

        if self._is_circular:
            self.get_logger().info(
                f'Circular trajectory detected (start≈end within {self._circular_threshold} m). '
                f'Will keep {self._circular_loops_ahead} loops ahead and refresh at half path.'
            )
        else:
            self.get_logger().info('Linear trajectory detected. Will follow once.')

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

    def _odom_cb(self, msg: Odometry):
        self._current_position = (
            msg.pose.pose.position.x,
            msg.pose.pose.position.y,
        )

        if not self._is_circular or self._send_in_progress or self._active_request_id is None:
            return

        nearest_index = self._find_nearest_waypoint_index()
        self._update_circular_progress(nearest_index)

    def _detect_circular(self) -> bool:
        if len(self._waypoints) < 2:
            return False

        first = self._waypoints[0]
        last = self._waypoints[-1]
        return math.hypot(first[0] - last[0], first[1] - last[1]) < self._circular_threshold

    def _distance_between_waypoints(self, first: tuple, second: tuple) -> float:
        return math.hypot(first[0] - second[0], first[1] - second[1])

    def _compute_path_length(self, waypoints: list, wrap: bool = False) -> float:
        if len(waypoints) < 2:
            return 0.0

        total = 0.0
        for idx in range(len(waypoints) - 1):
            total += self._distance_between_waypoints(waypoints[idx], waypoints[idx + 1])

        if wrap:
            total += self._distance_between_waypoints(waypoints[-1], waypoints[0])

        return total

    def _prepare_circular_waypoints(self, waypoints: list) -> list:
        if len(waypoints) < 3:
            return waypoints

        average_spacing = self._compute_path_length(waypoints) / max(len(waypoints) - 1, 1)
        closure_distance = self._distance_between_waypoints(waypoints[0], waypoints[-1])

        if closure_distance <= max(average_spacing * 2.0, 1e-3):
            return waypoints[:-1]

        return waypoints

    def _find_nearest_waypoint_index(self) -> int:
        if self._current_position is None:
            return 0

        robot_x, robot_y = self._current_position
        best_index = 0
        best_distance_sq = float('inf')

        for idx, waypoint in enumerate(self._waypoints):
            dx = waypoint[0] - robot_x
            dy = waypoint[1] - robot_y
            distance_sq = dx * dx + dy * dy
            if distance_sq < best_distance_sq:
                best_distance_sq = distance_sq
                best_index = idx

        return best_index

    def _update_circular_progress(self, nearest_index: int):
        if self._last_nearest_index is None:
            self._last_nearest_index = nearest_index
            return

        num_waypoints = len(self._waypoints)
        delta = (nearest_index - self._last_nearest_index) % num_waypoints

        # Ignore large backward / noisy jumps when estimating forward progress.
        if delta > num_waypoints // 2:
            self._last_nearest_index = nearest_index
            return

        self._last_nearest_index = nearest_index
        self._circular_progress_points += delta

        if self._circular_progress_points < self._refresh_after_points:
            return

        self.get_logger().info(
            f'Circular refresh triggered after {self._circular_progress_points} waypoint steps.'
        )
        self._send_path(reason='half path completed')

    def _build_path(self, waypoints: list) -> Path:
        path = Path()
        path.header.frame_id = 'map'
        path.header.stamp = self.get_clock().now().to_msg()

        for x, y, qx, qy, qz, qw in waypoints:
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

    def _build_circular_path(self) -> tuple[Path, int, int]:
        start_index = self._find_nearest_waypoint_index()
        num_waypoints = len(self._waypoints)
        repeated_waypoints = []
        full_loops = int(math.floor(self._circular_loops_ahead))
        fractional_loop = self._circular_loops_ahead - full_loops
        total_waypoints = full_loops * num_waypoints

        if fractional_loop > 0.0:
            total_waypoints += max(1, int(round(num_waypoints * fractional_loop)))

        for offset in range(total_waypoints):
            repeated_waypoints.append(
                self._waypoints[(start_index + offset) % num_waypoints]
            )

        return self._build_path(repeated_waypoints), start_index, total_waypoints

    def _start(self):
        if self._started:
            return
        self._started = True

        self.get_logger().info('Waiting for /follow_path action server...')
        self._client.wait_for_server()
        self._send_path()

    def _send_path(self, reason: str = ''):
        if self._send_in_progress:
            return

        if self._is_circular:
            path, start_index, total_waypoints = self._build_circular_path()
            label = (
                f'circular path from waypoint {start_index} '
                f'with {len(path.poses)} poses'
            )
            self._last_nearest_index = start_index
            self._circular_progress_points = 0
            self._refresh_after_points = max(1, total_waypoints // 2)
        else:
            path = self._build_path(self._waypoints)
            label = f'path with {len(path.poses)} poses'

        goal = FollowPath.Goal()
        goal.path = path
        goal.controller_id = self._controller_id
        goal.goal_checker_id = self._goal_checker_id

        message = f'Sending {label} [controller: {self._controller_id}]'
        if reason:
            message += f' ({reason})'
        self.get_logger().info(message)

        self._send_in_progress = True
        self._request_id += 1
        request_id = self._request_id
        send_future = self._client.send_goal_async(
            goal,
            feedback_callback=self._feedback_cb
        )
        send_future.add_done_callback(
            lambda future, current_id=request_id: self._goal_response_cb(future, current_id)
        )

    def _goal_response_cb(self, future, request_id: int):
        self._send_in_progress = False
        self._goal_handle = future.result()
        if not self._goal_handle.accepted:
            self.get_logger().error('Path rejected by controller server.')
            return

        self._active_request_id = request_id
        self.get_logger().info('Path accepted. Following...')
        result_future = self._goal_handle.get_result_async()
        result_future.add_done_callback(
            lambda future, current_id=request_id: self._result_cb(future, current_id)
        )

    def _feedback_cb(self, feedback_msg):
        pass

    def _result_cb(self, future, request_id: int):
        if request_id != self._active_request_id:
            return

        status = future.result().status
        if status == GoalStatus.STATUS_SUCCEEDED:
            if self._is_circular:
                self.get_logger().info(
                    'Circular goal reached before refresh. Resending current circular path.'
                )
                self._send_path(reason='goal reached')
                return

            self.get_logger().info('Path following completed successfully.')
        else:
            self.get_logger().warn(f'Path following ended with status: {status}')


def main(args=None):
    rclpy.init(args=args)
    node = WaypointFollowerNode()
    rclpy.spin(node)
    rclpy.shutdown()
