from utils.html_downloader import HTMLDownloader
from utils.html_parser import HTMLParser


class MinistryExplorer:
    def __init__(self, ministry_name, url):
        self.ministry_name = ministry_name
        self.url = url

    def get_name_department(self, url, parent_department):
        downloader = HTMLDownloader(url)
        html_content = downloader.download_html()
        parser = HTMLParser(html_content)
        name_info = parser.extract_name_info(department=parent_department, url=url)
        department_info = parser.extract_departments(
            parent_department=parent_department
        )

        return name_info, department_info

    def traverse_departments(
        self, url, department_name, names=None, departments=None, depth=0
    ):
        if names is None:
            names = []
        if departments is None:
            departments = []

        department = {"link": url, "name": department_name}
        departments.append(department)

        name_info, department_info = self.get_name_department(
            url=url, parent_department=department_name
        )
        if name_info:
            names += name_info

        if department_info:
            department["children"] = []
            for child_department in department_info:
                print(f"{'  ' * depth}{child_department['name']}")
                child_names, child_departments = self.traverse_departments(
                    url=child_department["link"],
                    department_name=child_department["name"],
                    depth=depth + 1,
                )
                names += child_names
                department["children"].extend(child_departments)

        return names, departments

    def explore_ministries(self):
        names, departments = self.traverse_departments(self.url, self.ministry_name)
        return names, departments
