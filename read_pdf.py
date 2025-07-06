import os
import pdfplumber
import pandas as pd
import argparse
import pytesseract
from tqdm import tqdm
from multiprocessing import freeze_support, Pool
from PIL import Image
# import easyocr
pytesseract.pytesseract.tesseract_cmd = "D:\\tesseract-ocr\\tesseract.exe"
def read_picture(path):
    image = Image.open(path)
    # 使用pytesseract进行OCR识别
    text = pytesseract.image_to_string(image, lang='chi_sim')
    text = text.replace(' ', '')
    d = {}
    kaohao = None
    for line in text.split('\n'):
        if "考生号" in line:
            lsp = line.strip().split(":")
            kaohao = lsp[1].split('姓名')[0]
            name = lsp[2].split('性别')[0]
            assert kaohao not in d, kaohao
            d[kaohao] = {'name': name}
        elif "身份证号" in line:
            idnum = line.strip().split(':')[1]
            assert kaohao is not None
            d[kaohao]['id'] = idnum
    res = []
    fname = os.path.basename(path)
    for kaohao, item in d.items():
        res.append([fname, kaohao, item['name'], item['id']])
    return res

def read_pdf(path):
    d = {}
    with pdfplumber.open(path) as pdf:
        for page_num, page in enumerate(pdf.pages):
            text = page.extract_text()
            kaohao = None
            for line in text.split('\n'):
                if "考生号" in line:
                    lsp = line.strip().split()
                    kaohao = lsp[0].split('：')[1]
                    name = lsp[1].split('：')[1]
                    assert kaohao not in d, kaohao
                    d[kaohao] = {'name': name}
                elif "身份证号" in line:
                    idnum = line.strip().split('：')[1]
                    assert kaohao is not None
                    d[kaohao]['id'] = idnum
    res = []
    fname = os.path.basename(path)
    for kaohao, item in d.items():
        res.append([fname, kaohao, item['name'], item['id']])
    return res

def read_worker(path):
    try:
        if path.endswith("pdf"):
            return read_pdf(path)
        elif path.endswith("jpg") or path.endswith("png"):
            return read_picture(path)
    except:
        print(f"{path} 解析失败")
    return []
def write_excel(reslist, outpath, total_fnames):
    # 创建一个 DataFrame
    data1 = {
        "文件名": [],
        "考生号": [],
        "姓名": [],
        "身份证号": []
    }
    for item in reslist:
        if len(item) == 0:
            continue
        item = item[0]
        if 'cid:0' in item[1]:
            continue
        data1["文件名"].append(item[0])
        data1["考生号"].append(item[1])
        data1["姓名"].append(item[2])
        data1["身份证号"].append(item[3])

    df1 = pd.DataFrame(data1)

    # 统计失败的文件
    success_fnames = set(data1["文件名"])
    failed_fnames = total_fnames - success_fnames
    data2 = {
        "解析失败": list(failed_fnames)
    }
    df2 = pd.DataFrame(data2)

    # 写入 Excel 文件
    outpath = os.path.abspath(outpath)
    outdir = os.path.dirname(outpath)
    os.makedirs(outdir, exist_ok=True)
    with pd.ExcelWriter(outpath) as writer:
        df1.to_excel(writer, index=False, sheet_name="准考证")
        df2.to_excel(writer, index=False, sheet_name="失败文件")

    print(f"Excel文件已保存至{outpath}！{len(failed_fnames)}个失败")


def main(args):
    freeze_support()  # 如果你打算打包成可执行文件，则需要此行
    pathlist = []
    total_fnames = set()
    for fname in os.listdir(args.input_dir):
        # if not fname.endswith("jpg"):
        #     continue
        path = f"{args.input_dir}/{fname}"
        pathlist.append(path)
        total_fnames.add(fname)

    if args.njobs > 1:
        # 创建线程池并启动任务
        with Pool(processes=args.njobs) as pool:  # 根据你的CPU核心数调整processes数量
            results =  list(tqdm(pool.imap(read_worker, pathlist), total=len(pathlist)))
    else:
        results = []
        for path in pathlist:
            result = read_worker(path)
            results.append(result)
    print("All tasks have finished.")
    # 打印结果或进行其他操作
    write_excel(results, args.output_file, total_fnames)

# 定义任务函数，接受参数
if __name__ == '__main__':
    parser = argparse.ArgumentParser("usage: python read_pdf.py /path/to/pdfdir /path/to/output.xlsx")
    parser.add_argument("input_dir", type=str, help="dir path of files")
    parser.add_argument("output_file", type=str, help="path to output excel, for example, output.xlsx")
    parser.add_argument("--njobs", type=int, default=16, help="num workers")
    args = parser.parse_args()
    main(args)

# home = "C:/Users/志浩/Documents/WeChat Files/wxid_3620966225512/FileStorage/File/2025-03/常萍萍"
# python read_pdf.py home home/../output.xlsx