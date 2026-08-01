from bs4 import BeautifulSoup
from logger import get_logger

logger = get_logger(__name__)


class HTMLParser:
    def __init__(self, html):
        self.html = html

    # Get Name Information on the Page

    def _extract_position(self, li_tag):
        if li_tag:
            rank_div = li_tag.find(class_="rank")
            if rank_div:
                return rank_div.text.strip()
        return ""

    def _extract_personnel_name(self, li_tag):
        if li_tag:
            name_div = li_tag.find(class_="name")
            if name_div:
                return name_div.text.strip()
        return ""

    def _extract_email(self, li_tag):
        if li_tag:
            email_div = li_tag.find(class_="email")
            if email_div and email_div.div:
                return email_div.div.text.strip()
        return ""

    # this applies more to first pages, with toggle
    def _extract_section_toggle(self, soup):
        section_divs = []

        # Find all section-toggle divs
        section_toggle_divs = soup.find_all("div", class_="section-toggle")

        for section_toggle_div in section_toggle_divs:
            # Find the section-body div within each section-toggle div
            section_body_div = section_toggle_div.find("div", class_="section-body")
            section_divs.append(section_body_div)

        return section_divs

    # this applies more to subsequent sections
    def _extract_section_info(self, soup):
        # Find all section-info divs
        section_info_divs = soup.find_all("div", class_="section-info")

        return section_info_divs

    def extract_name_info(self, department, url):
        # From 1 Jul 2026, SGDI only lists senior-personnel contact details
        # (GOIS scam mitigation) — junior/operational staff entries are being
        # progressively withdrawn from department pages, so this can
        # legitimately return far fewer rows, or [] for depts with no senior
        # staff shown, without indicating a parsing error. extract_departments()
        # below reads department links, not personnel, so the department tree
        # (names + child links) is unaffected and continues to populate normally.
        soup = BeautifulSoup(self.html, "html.parser")
        info_list = []
        section_divs = []

        section_divs += self._extract_section_toggle(soup)
        section_divs += self._extract_section_info(soup)

        for section_div in section_divs:
            if section_div:
                # Find the <ul> tag within the section-body div
                ul_tag = section_div.find("ul")

                if ul_tag:
                    # Find all <li> tags within the <ul> tag
                    li_tags = ul_tag.find_all("li")

                    for li_tag in li_tags:
                        position = self._extract_position(li_tag)
                        name = self._extract_personnel_name(li_tag)
                        email = self._extract_email(li_tag)
                        if position and name: # no need email to be present
                            info_list.append(
                                {
                                    "position": position,
                                    "name": name,
                                    "email": email,
                                    "department": department,
                                    "url": url
                                }
                            )

        return info_list

    # Get Sub-Divisions

    def _extract_department_name(self, li_tag):
        department_name = li_tag.text.strip()
        words = department_name.split()
        return " ".join(words)

    def _extract_link(self, li_tag):
        a_tag = li_tag.find("a")
        if a_tag is None:
            return None
        return a_tag.get("href")

    def extract_departments(self, parent_department):
        departments = []
        soup = BeautifulSoup(self.html, "html.parser")
        div_tags = soup.find_all("div", class_="tab-pane")
        for div_tag in div_tags:
            ul_tag = div_tag.find("ul", class_="section-listing")
            if ul_tag:
                for li_tag in ul_tag.find_all("li"):
                    department_name = self._extract_department_name(li_tag)
                    department_link = self._extract_link(li_tag)
                    if department_link is None:
                        # SGDI renders some sub-units as plain, non-clickable
                        # text (red, cursor:not-allowed) with no <a> tag, e.g.
                        # "OTHER PA UNITS" / "INFRASTRUCTURE" under People's
                        # Association. There's no page to crawl for these, but
                        # we still record the name with link=None so the
                        # departments table reflects the full org tree.
                        logger.warning(
                            f"Department '{department_name}' under '{parent_department}' has no link; recording with link=None"
                        )
                    departments.append(
                        {
                            "name": department_name,
                            "link": department_link,
                            "parent": parent_department,
                        }
                    )
        return departments
