from datetime import datetime
from logger import get_logger
from utils.html_downloader import HTMLDownloader
from utils.html_parser import HTMLParser

logger = get_logger(__name__)


class MinistryExplorer:
    def __init__(self, ministry_name, url):
        self.ministry_name = ministry_name
        self.url = url
        self.start_time = None
        self.end_time = None
        self.departments_processed = 0
        self.total_top_level = 0
        self.current_top_level = 0

    def get_name_department(self, url, parent_department):
        logger.debug(f"Fetching data from: {url}")
        downloader = HTMLDownloader(url)
        html_content = downloader.download_html()
        
        if html_content is None:
            logger.warning(f"Failed to download HTML from {url}")
            return [], []
        
        parser = HTMLParser(html_content)
        name_info = parser.extract_name_info(department=parent_department, url=url)
        department_info = parser.extract_departments(
            parent_department=parent_department
        )
        
        logger.debug(f"Found {len(name_info)} names and {len(department_info)} sub-departments")
        return name_info, department_info

    def _format_time(self, seconds):
        """Format seconds into human readable time."""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            mins = seconds // 60
            secs = seconds % 60
            return f"{mins:.0f}m {secs:.0f}s"
        else:
            hours = seconds // 3600
            mins = (seconds % 3600) // 60
            return f"{hours:.0f}h {mins:.0f}m"

    def _get_progress_stats(self):
        """Calculate and return progress statistics."""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        
        if self.departments_processed > 0:
            avg_time = elapsed / self.departments_processed
            
            # Estimate remaining based on top-level progress
            if self.current_top_level > 0 and self.total_top_level > 0:
                # Rough estimate: assume each top-level section takes similar time
                time_per_section = elapsed / self.current_top_level
                remaining_sections = self.total_top_level - self.current_top_level
                est_remaining = remaining_sections * time_per_section
                return f"[{self.current_top_level}/{self.total_top_level}] ~{self._format_time(est_remaining)} left"
        
        return ""

    def traverse_departments(
        self, url, department_name, names=None, departments=None, depth=0, is_top_level=False
    ):
        if names is None:
            names = []
        if departments is None:
            departments = []

        department = {"link": url, "name": department_name}
        departments.append(department)
        self.departments_processed += 1

        name_info, department_info = self.get_name_department(
            url=url, parent_department=department_name
        )
        if name_info:
            names += name_info

        # If this is the root, count top-level departments
        if depth == 0 and department_info:
            self.total_top_level = len(department_info)

        if department_info:
            department["children"] = []
            for i, child_department in enumerate(department_info):
                indent = "  " * depth
                
                # Track top-level progress
                if depth == 0:
                    self.current_top_level = i + 1
                    progress = self._get_progress_stats()
                    logger.info(f"{progress} {indent}→ {child_department['name']}")
                else:
                    logger.info(f"{indent}→ {child_department['name']}")
                
                child_names, child_departments = self.traverse_departments(
                    url=child_department["link"],
                    department_name=child_department["name"],
                    depth=depth + 1,
                    is_top_level=(depth == 0),
                )
                names += child_names
                department["children"].extend(child_departments)

        return names, departments

    def explore_ministries(self):
        logger.info(f"Starting exploration of {self.ministry_name}")
        self.start_time = datetime.now()
        self.departments_processed = 0
        
        names, departments = self.traverse_departments(self.url, self.ministry_name)
        
        self.end_time = datetime.now()
        elapsed = (self.end_time - self.start_time).total_seconds()
        logger.info(f"Exploration complete: {len(names)} names, {self.departments_processed} departments in {self._format_time(elapsed)}")
        return names, departments

    def get_exploration_duration(self):
        """Return the duration of the last exploration in seconds."""
        if self.start_time and self.end_time:
            return round((self.end_time - self.start_time).total_seconds(), 2)
        return None
