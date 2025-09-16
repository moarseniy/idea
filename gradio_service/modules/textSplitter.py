from langchain.text_splitter import RecursiveCharacterTextSplitter
class TextSplitter:
  def __init__(self):
    self.text_splitter = RecursiveCharacterTextSplitter(
        separators=[".", "?", "!"],
        chunk_size=2000,
        chunk_overlap=500,
        length_function=len,
        is_separator_regex=False,
        keep_separator="end"
        )
  def split(self, text:str):
    return self.text_splitter.split_text(text)