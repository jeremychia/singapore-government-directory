class PrefixExtractor:
    COMMON_PREFIXES = ["ms", "mr", "dr", "mdm", "miss", "mrs"]
    ACADEMIC_PREFIXES = [
        "adj",
        "prof",
        "adjunct",
        "asst",
        "a/prof",
        "assoc",
        "professor",
        "a/p",
    ]
    RANK_PREFIXES = [
        "insp",
        "asp",
        "dsp",
        "supt(1a)",
        "supt(1)",
        "supt",
        "dac",
        "ac",
        "sac",
        "dc",
        "cp",
        "comr",
        "mwo",
        "swo",
        "cwo",
        "cpt",
        "capt",
        "maj",
        "ltc",
        "sltc",
        "col",
        "col(dr)",
        "bg",
        "mg",
        "me5",
        "me6",
        "me7",
        "me8",
        "me9",
        "radm",
    ]
    CIVIL_TITLE_PREFIXES = ["pdj", "dj", "ci", "ar", "ag"]
    FIRST_PREFIXES = (
        COMMON_PREFIXES + ACADEMIC_PREFIXES + RANK_PREFIXES + CIVIL_TITLE_PREFIXES
    )
    SECOND_PREFIXES = [
        "a/p",
        "prof",
        "a/prof",
        "associate",
        "asst",
        "(ret)",
        "(v)",
        "(dr)",
        "(ns)",
        "(1a)",
        "1a",
    ]
    THIRD_PREFIXES = [
        "(mr)",
        "(ms)",
        "(mrs)",
        "(mdm)",
        "prof",
        "(dr)",
    ]
    # manual exclusions, because they break the logic
    EXCLUDE_NAMES = ["Egwin LIAN (MR)", "Amos Kow (MR)", "Gladys CHIAM (Ms)"]

    @staticmethod
    def extract(name):
        parts = name.split()
        prefix = ""

        if parts[0].lower().replace(".", "") in PrefixExtractor.FIRST_PREFIXES:
            prefix = parts[0]

        if (
            len(parts) > 1
            and parts[1].lower().replace(".", "") in PrefixExtractor.SECOND_PREFIXES
        ):
            prefix = " ".join(parts[:2])

        if (
            len(parts) > 2
            and parts[2].lower().replace(".", "") in PrefixExtractor.THIRD_PREFIXES
            and name not in PrefixExtractor.EXCLUDE_NAMES
        ):
            prefix = " ".join(parts[:3])

        if prefix == "":
            return None
        else:
            return prefix.rstrip(".")


class PostfixExtractor:
    AWARDS = [
        "DUNU",
        "DUBC",
        "PGP",
        "PJG",
        "BBM",
        "PPA(E)",
        "PPA (E)",
        "PPA(P)",
        "PPA (P)",
        "PPA(G)",
        "PPA (G)",
        "PK",
        "PBM",
        "P Kepujian",
        "P. Kepujian",
        "PB",
        "PBS",
        "CRM",
    ]

    @staticmethod
    def extract(string):
        min_position = len(string)  # Initialise with length of string

        for substring in PostfixExtractor.AWARDS:
            position = string.find(substring)
            if position != -1 and position < min_position:
                min_position = position

        if min_position == len(string):
            return None  # If no substring found, return None
        else:
            return string[min_position:]


class NameCleaner:
    @staticmethod
    def remove_prefix_postfix(name, prefix, postfix):
        # Handle NaN values - check if they're strings and not NaN
        if isinstance(prefix, str) and prefix.strip():
            name = name[len(prefix) + 1 :]  # Remove prefix along with space
        if isinstance(postfix, str) and postfix.strip():
            name = name[: -len(postfix)]  # Remove postfix

        return name.strip().rstrip(",")


class NameProcessor:
    @staticmethod
    def clean_name(row):
        return NameCleaner.remove_prefix_postfix(
            row["name"], row["prefix"], row["postfix"]
        )

    @classmethod
    def process_names(cls, names_df):
        names_df["prefix"] = names_df["name"].apply(PrefixExtractor.extract)
        names_df["postfix"] = names_df["name"].apply(PostfixExtractor.extract)
        names_df["extracted_name"] = names_df.apply(cls.clean_name, axis=1)
