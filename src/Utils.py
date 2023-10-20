def formatXML(filename: str):
    XML_translator = [["'",  '&apos;'], ['"',  '&quot;'], ["&",  '&amp;'],
                      ["<",  '&lt;'],   [">",  '&gt;'],   ["\r", '&#13;'],
                      ['\n', '&#10;']]
    for source_char, target_char in XML_translator:
        filename = filename.replace(source_char, target_char)
    return filename

def splitList(some_list: list, lenght: int):
    '''Devide some list into chunks of fixed lenght'''
    return (some_list[i:i + lenght] for i in range(0, len(some_list), lenght))