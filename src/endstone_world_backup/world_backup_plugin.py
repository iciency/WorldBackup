import datetime
import os
import traceback
import zipfile
from endstone.plugin import Plugin
from endstone.command import Command, CommandSender

class WorldBackupPlugin(Plugin):
    api_version = "0.6"
    commands = {
        "backup": {
            "description": "Backs up the server world.",
            "aliases": ["wb"],
        }
    }

    def __init__(self):
        super().__init__()
        self.auto_backup_task = None
        self.is_backing_up = False  # Lock to prevent concurrent backups

    def on_load(self):
        self.logger.info("WorldBackupPlugin has been loaded!")

    def on_enable(self):
        self.logger.info("WorldBackupPlugin has been enabled!")
        self._validate_config()
        self._schedule_auto_backup()

    def _validate_config(self):
        """Validates the config file, creating or updating it as necessary."""
        current_config_version = 1
        config_path = os.path.join(self.data_folder, "config.toml")

        if not os.path.exists(config_path):
            self.logger.info(f"Configuration file not found. Creating a new one at: {config_path}")
            self.save_default_config()
            self.reload_config()
            return

        self.reload_config()
        loaded_version = self.config.get("config-version")

        if loaded_version != current_config_version:
            self.logger.warning("Configuration file version mismatch or missing!")
            self.logger.warning(f"Found version '{loaded_version}', expected '{current_config_version}'.")
            
            backup_path = os.path.join(self.data_folder, "config.old.toml")
            try:
                os.rename(config_path, backup_path)
                self.logger.info(f"Your old configuration has been backed up to: {backup_path}")
            except OSError as e:
                self.logger.error(f"Could not back up old configuration file: {e}")
                return

            self.logger.info("A new configuration file will be generated.")
            self.save_default_config()
            self.reload_config()

    def on_disable(self):
        self.logger.info("WorldBackupPlugin has been disabled!")
        if self.auto_backup_task:
            self.auto_backup_task.cancel()
            self.logger.info("Auto-backup task cancelled.")

    def on_command(self, sender: CommandSender, command: Command, args: list[str]) -> bool:
        if command.name.lower() == "backup":
            return self._execute_backup(sender, is_auto=False)
        return False

    def _schedule_auto_backup(self):
        # Cancel any existing task before scheduling a new one (for reloads)
        if self.auto_backup_task:
            self.auto_backup_task.cancel()
            self.auto_backup_task = None

        auto_backup_config = self.config.get("auto-backup", {})
        is_enabled = auto_backup_config.get("enabled", False)
        
        if is_enabled:
            interval_hours = auto_backup_config.get("interval-hours", 1)
            if not isinstance(interval_hours, (int, float)) or interval_hours <= 0:
                self.logger.warning(f"Invalid auto-backup interval: {interval_hours}. Must be a positive number.")
                return

            self.logger.info(f"Auto-backup enabled. Interval: {interval_hours} hour(s).")
            # 1 hour = 3600 seconds = 3600 * 20 ticks = 72000 ticks
            interval_ticks = int(interval_hours * 3600 * 20)

            def backup_task():
                self.logger.info("Running scheduled auto-backup...")
                self._execute_backup(self.server.command_sender, is_auto=True)

            self.auto_backup_task = self.server.scheduler.run_task(self, backup_task, delay=interval_ticks, period=interval_ticks)
        else:
            self.logger.info("Auto-backup is disabled in the config.")

    def _execute_backup(self, sender: CommandSender, is_auto: bool = False) -> bool:
        if self.is_backing_up:
            message = "A backup is already in progress. Please wait for it to complete."
            if is_auto:
                self.logger.warning(message)
            else:
                sender.send_message(message)
            return False

        self.is_backing_up = True
        if not is_auto:
            sender.send_message("Starting world backup...")
        
        try:
            # Construct the world path from server CWD and level name
            level_name = sender.server.level.name
            # Most Bedrock servers store worlds in a 'worlds' sub-directory
            world_path = os.path.join(os.getcwd(), "worlds", level_name)

            if not os.path.isdir(world_path):
                message = f"Error: World directory not found at '{world_path}'"
                sender.send_message(message)
                self.logger.error(message)
                return False

            backup_path_str = self.config.get("backup-path")

            if backup_path_str:
                if not os.path.isabs(backup_path_str):
                    backup_dir = os.path.join(os.getcwd(), backup_path_str)
                else:
                    backup_dir = backup_path_str
            else:
                backup_dir = os.path.join(self.data_folder, "backups")

            try:
                os.makedirs(backup_dir, exist_ok=True)
            except PermissionError:
                self.logger.error(f"Permission denied to create backup directory at: {backup_dir}")
                if not is_auto:
                    sender.send_message("Permission denied to create backup directory. Check console for details.")
                self.is_backing_up = False
                return False

            timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            backup_file_name = f"world_backup_{timestamp}.zip"
            backup_path = os.path.join(backup_dir, backup_file_name)

            files_skipped = 0
            with zipfile.ZipFile(backup_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(world_path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, world_path)
                        try:
                            zipf.write(file_path, arcname)
                        except FileNotFoundError:
                            files_skipped += 1
                            self.logger.warning(f"Skipped a file that was deleted during backup: {arcname}")

            success_message = f"World backup successful! Saved to {backup_path}"
            if files_skipped > 0:
                success_message += f" ({files_skipped} files were skipped as they were modified during backup)"
            if is_auto:
                self.logger.info(success_message)
            else:
                sender.send_message(success_message)
            
            # Clean up old backups
            self._manage_backups(backup_dir)
            
            return True
        except Exception as e:
            error_message = f"An unhandled error occurred during backup: {e}\n{traceback.format_exc()}"
            self.logger.error(error_message)
            if not is_auto:
                sender.send_message("An unhandled error occurred during backup. Check the server console for details.")
            return False
        finally:
            self.is_backing_up = False  # Release the lock

    def _manage_backups(self, backup_dir: str):
        backup_management_config = self.config.get("backup-management", {})
        max_backups = backup_management_config.get("max-backups", 10)

        if not isinstance(max_backups, int) or max_backups <= 0:
            return  # Automatic deletion is disabled or invalid

        try:
            self.logger.info(f"Checking for old backups to prune (limit: {max_backups})...")
            # Get all .zip files in the backup directory
            backups = [f for f in os.listdir(backup_dir) if f.endswith('.zip') and f.startswith('world_backup_')]
            
            # Sort backups by name, which corresponds to the timestamp (oldest first)
            backups.sort()

            if len(backups) > max_backups:
                files_to_delete = backups[:len(backups) - max_backups]
                self.logger.info(f"Found {len(files_to_delete)} old backup(s) to delete.")
                for filename in files_to_delete:
                    file_path = os.path.join(backup_dir, filename)
                    try:
                        os.remove(file_path)
                        self.logger.info(f"Deleted old backup: {filename}")
                    except FileNotFoundError:
                        self.logger.warning(f"Tried to delete old backup but it was already gone: {filename}")
                    except Exception as e:
                        self.logger.error(f"Error deleting old backup {filename}: {e}")
        except Exception as e:
            self.logger.error(f"An error occurred while managing old backups: {e}\n{traceback.format_exc()}")
