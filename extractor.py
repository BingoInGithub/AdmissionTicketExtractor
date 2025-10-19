import os
import pdfplumber
import pandas as pd
import argparse
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
    base_name = os.path.basename(pdf_path)
    combined_image_path = f"{logdir}/{base_name}.jpg"
    if os.path.exists(combined_image_path):
        return combined_image_path

    # 打开 PDF 文件
    doc = fitz.open(pdf_path)

    image_count = 0
    image_paths = []
    # 遍历每一页
    for page_num in range(len(doc)):
        page = doc.load_page(page_num)
        image_list = page.get_images(full=True)  # 获取该页所有图像

        # print(f"第 {page_num + 1} 页发现 {len(image_list)} 张图片")

        # 遍历所有图像
        for img_index, img in enumerate(image_list):
            xref = img[0]  # XREF 是图像在 PDF 中的唯一标识符
            base_image = doc.extract_image(xref)
            image_bytes = base_image["image"]  # 图像二进制数据
            image_ext = base_image["ext"]      # 图像扩展名 (png, jpeg 等)

            # 写入图像文件
            image_path = f"{logdir}/{base_name}_{page_num}_{img_index}.{image_ext}"

            with open(image_path, "wb") as img_file:
                img_file.write(image_bytes)

            image_count += 1
            image_paths.append(image_path)
            # print(f"已保存: {image_path}")

    doc.close()
    # print(f"\n共提取 {image_count} 张图片")

    images = [Image.open(image_path) for image_path in image_paths]

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
    
    combined_image.save(combined_image_path)
    # print(f"图片已合并并保存为: {output_path}")
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
            text = page.extract_text()
            kaohao = None
            tsp = text.split('\n')
            for i,line in enumerate(tsp):
                if "考生号" in line:
                    lsp = line.strip().split()
                    kaohao = lsp[0].split('：')[1]
                    name = lsp[1].split('：')[1]
                    assert kaohao not in d, kaohao
                    d[kaohao] = {'name': name}
                elif "身份证号" in line:
                    idnum = line.strip().split('：')[1]
                    assert kaohao is not None
                    if idnum == '':
                        idnum = tsp[i+1].strip()
                        assert idnum.isdigit() and len(idnum) == 18
                    d[kaohao]['id'] = idnum
    if len(d) == 0:
        path = extract_images_from_pdf(path, logdir)
        return read_picture(idx, fname, path, ocr)
    res = []
    # fname = os.path.basename(path)
    for kaohao, item in d.items():
        res.append([fname, kaohao, item['name'], item['id']])
    return res

def read_worker(param):
    idx, path, ocr, logdir = param
    try:
        fname = os.path.basename(path)
        suffix = path.split(".")[-1].lower()
        if suffix in ["pdf"]:
            return read_pdf(idx, fname, path, ocr, logdir)
        elif suffix in ["jpg", "jpeg", "png"]:
            return read_picture(idx, fname, path, ocr)
        else:
            raise Exception(f"not support format: {path}")
    except:
        print(f"{path} 解析失败")
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
        if len(item) == 0:
            continue
        item = item[0]
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

# 定义任务函数，接受参数
if __name__ == '__main__':
    parser = argparse.ArgumentParser("usage: python read_pdf.py /path/to/pdfdir /path/to/output.xlsx")
    parser.add_argument("indir", type=str, help="dir path of files")
    parser.add_argument("outdir", type=str, help="path to output")
    parser.add_argument("--njobs", type=int, default=16, help="num workers")
    args = parser.parse_args()
    main(args)
