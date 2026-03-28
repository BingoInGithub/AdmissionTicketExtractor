import os
import pdfplumber
import pandas as pd
import argparse
import traceback
import io 
import contextlib
from datetime import datetime
from tqdm import tqdm
from multiprocessing import freeze_support, Pool
from aliyun import OCR
import fitz  # PyMuPDF
from PIL import Image

def combine_images_vertically(image_folder, output_path):
    # 获取所有图片路径（按顺序排序）
    image_files = sorted(
        [f for f in os.listdir(image_folder) if f.lower().endswith(('.png', '.jpg', '.jpeg'))],
        key=lambda x: int(''.join(filter(str.isdigit, x)))  # 按数字排序
    )

    if not image_files:
        print("没有找到图片文件")
        return

    images = [Image.open(os.path.join(image_folder, img)) for img in image_files]

    # 获取最大宽度，总高度为所有图片高度之和
    max_width = max(img.width for img in images)
    total_height = sum(img.height for img in images)

    # 创建新图像，白色背景
    combined_image = Image.new('RGB', (max_width, total_height), (255, 255, 255))

    # 逐张粘贴
    y_offset = 0
    for img in images:
        combined_image.paste(img, (0, y_offset))
        y_offset += img.height

    # 保存结果
    combined_image.save(output_path)
    print(f"图片已合并并保存为: {output_path}")

def extract_images_from_pdf(pdf_path, logdir):
    os.makedirs(logdir, exist_ok=True)
    base_name = os.path.basename(pdf_path)
    combined_image_path = f"{logdir}/{base_name}.jpg"
    if os.path.exists(combined_image_path):
        return combined_image_path

    doc = fitz.open(pdf_path)
    rendered_images = []

    try:
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)

            # 渲染页面而不是提取内嵌图片
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
            img_path = f"{logdir}/{base_name}_page_{page_num}.png"
            pix.save(img_path)

            try:
                img = Image.open(img_path).convert("RGB")
                rendered_images.append(img)
            except Exception as e:
                print(f"页面渲染图打开失败: {img_path}, err={e}")

    finally:
        doc.close()

    if not rendered_images:
        raise Exception(f"PDF渲染失败，没有可用页面图像: {pdf_path}")

    max_width = max(img.width for img in rendered_images)
    total_height = sum(img.height for img in rendered_images)

    combined_image = Image.new('RGB', (max_width, total_height), (255, 255, 255))

    y_offset = 0
    for img in rendered_images:
        combined_image.paste(img, (0, y_offset))
        y_offset += img.height

    combined_image.save(combined_image_path)
    return combined_image_path


def read_picture(idx, fname, path, ocr):
    kaohao, name, idnum = None, None, None
    res = ocr.parse(idx, path)

    content = res['body']['Data']['Content']
    for value in content.split():
        if value.startswith("考生号"):
            kaohao = value.replace("：", ":").split(":")[1]
        elif value.startswith("姓名"):
            name = value.replace("：", ":").split(":")[1]
        elif value.startswith("身份证号"):
            idnum = value.replace("：", ":").split(":")[1]

    res = []
    # fname = os.path.basename(path)
    res.append([fname, kaohao, name, idnum])
    return res

def read_pdf(idx, fname, path, ocr, logdir):
    d = {}
    with pdfplumber.open(path) as pdf:
        for page_num, page in enumerate(pdf.pages):
            buf = io.StringIO()
            with contextlib.redirect_stderr(buf):
                text = page.extract_text()

            stderr_output = buf.getvalue()
            if "Could get FontBBox from font descriptor because None cannot be parsed as 4 floats" in stderr_output:
                print(f"页面{page_num}提取文本失败")
                continue
            kaohao = None
            tsp = text.split('\n')
            for i,line in enumerate(tsp):
                if "考生号" in line:
                    kaohao, name = line.replace(' ', '').split('性别：')[0].split('姓名：')
                    kaohao = kaohao.split('考生号：')[1]
                    assert kaohao not in d, kaohao
                    d[kaohao] = {'name': name}
                elif "身份证号" in line:
                    idnum = line.replace(' ', '').strip().split('：')[1]
                    assert kaohao is not None
                    if idnum == '':
                        idnum = tsp[i+1].strip()
                        assert idnum.isdigit() and len(idnum) == 18
                    d[kaohao]['id'] = idnum
    if len(d) == 0:
        path = extract_images_from_pdf(path, logdir)
        return read_picture(idx, fname, path, ocr)
    res = []
    for kaohao, item in d.items():
        res.append([fname, kaohao, item['name'], item['id']])
    return res

def read_worker(param):
    idx, path, ocr, logdir = param
    fname = os.path.basename(path)
    try:
        suffix = path.split(".")[-1].lower()
        if suffix in ["pdf"]:
            return read_pdf(idx, fname, path, ocr, logdir)
        elif suffix in ["jpg", "jpeg", "png"]:
            return read_picture(idx, fname, path, ocr)
        else:
            raise Exception(f"not support format: {path}")
    except Exception as e:
        error_msg = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 文件解析失败: {path}\n"
        error_msg += f"错误类型: {type(e).__name__}\n"
        error_msg += f"错误信息: {str(e)}\n"
        error_msg += f"堆栈跟踪:\n{traceback.format_exc()}\n"
        error_msg += "-" * 80 + "\n"

        # 写入错误日志文件
        error_log_path = os.path.join(logdir, "error.log")
        with open(error_log_path, "a", encoding="utf-8") as f:
            f.write(error_msg)

        print(f"{fname} 解析失败，详情见 {error_log_path}")
    return []


def write_excel(reslist, outdir, total_fnames):
    # 创建一个 DataFrame
    data1 = {
        "文件名": [],
        "考生号": [],
        "姓名": [],
        "身份证号": []
    }
    for item in reslist:
        try:
            if len(item) == 0:
                continue
            item = item[0]
            if item is None:
                continue
            if 'cid:0' in item[1]:
                continue
            valid = True
            for val in item:
                if val is None or len(val) == 0:
                    valid = False
                    break
            if not valid:
                continue
            data1["文件名"].append(item[0])
            data1["考生号"].append(item[1])
            data1["姓名"].append(item[2])
            data1["身份证号"].append(item[3])
        except Exception as e:
            error_msg = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] item写入失败: {item}\n"
            error_msg += f"错误类型: {type(e).__name__}\n"
            error_msg += f"错误信息: {str(e)}\n"
            error_msg += f"堆栈跟踪:\n{traceback.format_exc()}\n"
            error_msg += "-" * 80 + "\n"

    df1 = pd.DataFrame(data1)

    # 统计失败的文件
    success_fnames = set(data1["文件名"])
    failed_fnames = total_fnames - success_fnames
    data2 = {
        "解析失败": list(failed_fnames)
    }
    df2 = pd.DataFrame(data2)

    # 写入 Excel 文件
    outpath = os.path.abspath(f"{outdir}/result.xlsx")
    with pd.ExcelWriter(outpath) as writer:
        df1.to_excel(writer, index=False, sheet_name="准考证")
        df2.to_excel(writer, index=False, sheet_name="失败文件")

    print(f"{len(success_fnames)}个成功,{len(failed_fnames)}个失败.")
    print(f"Excel文件已保存至{outpath}！")


def main(args):
    freeze_support()  # 如果你打算打包成可执行文件，则需要此行
    os.makedirs(args.outdir, exist_ok=True)
    logdir = f"{args.outdir}/logs"
    os.makedirs(logdir, exist_ok=True)

    # 初始化错误日志文件
    error_log_path = os.path.join(logdir, "error.log")
    with open(error_log_path, "w", encoding="utf-8") as f:
        f.write(f"{'='*80}\n")
        f.write(f"准考证解析日志 - 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"输入目录: {args.indir}\n")
        f.write(f"输出目录: {args.outdir}\n")
        f.write(f"{'='*80}\n\n")

    ocr = OCR(logdir)
    params = []
    total_fnames = set()
    for idx, fname in enumerate(os.listdir(args.indir)):
        # if not fname.endswith("jpg"):
        #     continue
        path = os.path.abspath(f"{args.indir}/{fname}")
        params.append([idx, path, ocr, logdir])
        total_fnames.add(fname)

    if args.njobs > 1:
        # 创建线程池并启动任务
        with Pool(processes=args.njobs) as pool:  # 根据你的CPU核心数调整processes数量
            results =  list(tqdm(pool.imap(read_worker, params), total=len(params)))
    else:
        results = []
        for param in params:
            result = read_worker(param)
            results.append(result)
    print("All tasks have finished.")
    # 打印结果或进行其他操作
    write_excel(results, args.outdir, total_fnames)

    # 记录结束日志
    error_log_path = os.path.join(logdir, "error.log")
    with open(error_log_path, "a", encoding="utf-8") as f:
        f.write(f"\n{'='*80}\n")
        f.write(f"准考证解析日志 - 结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"{'='*80}\n")

# 定义任务函数，接受参数
if __name__ == '__main__':
    parser = argparse.ArgumentParser("usage: python read_pdf.py /path/to/pdfdir /path/to/output.xlsx")
    parser.add_argument("indir", type=str, help="dir path of files")
    parser.add_argument("outdir", type=str, help="path to output")
    parser.add_argument("--njobs", type=int, default=16, help="num workers")
    args = parser.parse_args()
    main(args)
