# AdmissionTicketExtractor
该项目用于自动解析专升本准考证中的考生姓名、身份证号和准考证号。
## 🛠 安装方法
```bash
pip install -r requirements.txt
```
此外，如果需要识别图片，需要前往阿里云官网购买OCR资源，并在当前目录下创建.env文件，将AK和SK填入。
.env文件内容如下
```bash
ALIBABA_CLOUD_ACCESS_KEY_ID="your_ak"
ALIBABA_CLOUD_ACCESS_KEY_SECRET="your_sk"
```
## 🔧 运行方式
```bash
python extractor.py <indir> <outdir>
```

其中indir包含准考证文件，支持pdf、png、jpg等格式；outdir为输出文件夹。
