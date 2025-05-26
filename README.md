# VNStock_pipeline
## I.Introduce
This project is created to demonstrate a real data pipeline. The Data is extracted from VietNam Stock Market.
 
II. Nguồn dữ liệu 
VNStock library is used in this project. This is a api wrapper.

III. Kiến trúc hệ thống
[Hình ảnh về kiến trúc hệ thống chi tiết](./assets/ProjectStructure.png)

1. VnStock Library : sourc
IV. Yêu cầu về công nghệ 
Python 3.10 is required 

## II.Data Source
Free API (library) from [Vnstock](https://github.com/thinh-vu/vnstock.git)

### 1. API : Vnstock().stock(symbol="ACB", source='VCI').listing.symbols_by_exchange()
```
List all the company currently on the stock exchange
Liệt kê mã cổ phiếu theo sàn đầy đủ 
Data returned: 
Symbol: mã cổ phiếu
Exchange: tên sàn ck 
Type : loại cổ phiếu
Organ_short_name: tên ngắn cty 
Organ_name : tên công ty đầy đủ 
```
### 2. API. Vnstock().stock(symbol=stock_code, source='VCI').trading.price_board( symbols_list=['VCB','ACB','TCB','BID'] )
```
Trả về bảng giá của mã ck tại thời điểm hiện tại , nếu k có thì sẽ trả về thời điểm gần nhất 
Dữ liệu trả về dạng dataframe 
<class 'pandas.core.frame.DataFrame'>
```

### 3. API : stock.quote.history(start='2020-01-01', end='2024-05-25')

```
Dữ liệu giá lịch sử thể hiện lịch sử giá giao dịch (đã điều chỉnh) của mã chứng khoán bất kỳ. Đây là dữ liệu được sử dụng để biểu diễn đồ thị kỹ thuật với định dạng tiêu chuẩn OHLCVT (Open, High, Low, Close, Volume, Time).

Tham số

symbol (không bắt buộc): nhập mã chứng khoán cần tra cứu, chấp nhận mã cổ phiếu, hợp đồng tương lai, chứng quyền, chỉ số, trái phiếu. Nếu không sử dụng tham số này thì thông tin mã chứng khoán được kế thừa từ câu lệnh khởi tạo class, ví dụ stock = Vnstock().stock(symbol='VCI', source='VCI'). Nếu khai báo mã chứng khoán mới thì thuộc tính symbol của đối tượng vnstock sẽ được cập nhật theo mã này. Không khuyến khích sử dụng tham số này vì dữ liệu trả vì đôi khi bạn không phân biệt được đang lấy dữ liệu từ mã nào, cần phải kiểm tra lại thuộc tính name như dưới đây để xác nhận.
start: Ngày kết thúc của truy vấn dữ liệu lịch sử. Định dạng YYYY-mm-dd
end: Ngày kết thúc của truy vấn dữ liệu lịch sử. Định dạng YYYY-mm-dd
interval: Khung thời gian lấy mẫu dữ liệu. Mặc định là 1D cho phép trả về dữ liệu lịch sử theo ngày. Các giá trị khác bao gồm:
1m: 1 phút
5m: 5 phút
15m: 15 phút
30m: 30 phút
1H: 1 giờ
1W: 1 tuần
1M: 1 tháng
to_df: Cho phép kết quả truy vấn trả về là pandas DataFrame hay JSON. Mặc định là True trả về DataFrame, đặt là False nếu muốn nhận kết quả dưới dạng JSON.
Thuộc tính dữ liệu Bạn có thể truy xuất thông tin thuộc tính của dữ liệu trả về với 2 thông tin sau:

name: Tên mã chứng khoán (df.name)
category: Tên loại tài sản mã chứng khoán đó thuộc về. (df.category)

Dữ liệu trả về 
<class 'pandas.core.frame.DataFrame'>

```

### 4. API dữ liệu khớp lệnh : stock.quote.intraday()

```
Dữ liệu intraday thể hiện giao dịch khớp lệnh trong phiên có độ chính xác đến hàng giây theo thời gian thực khi truy cứu trong khung giờ giao dịch 9:00 - 15:00 hàng ngày. Dữ liệu này hay được gọi với tên gọi intraday. Độ lớn của số điểm dữ liệu giao dịch mỗi ngày cho các mã chứng khoán lớn có thể lên đến ~15,000 dòng cho 1 ngày, thông thường dữ liệu này chỉ được cung cấp trong từng ngày

Tham số 

symbol (tuỳ chọn): Mặc định hàm này không cần tham số, thông tin mã chứng khoán được lấy từ thông tin symbol bạn đã chỉ định khi khởi tạo đối tượng stock ở trên. Nhập giá trị khác giá trị symbol đã có để chuyển đổi nhanh sang truy xuất dữ liệu mã chứng khoán bất kỳ mà không cần khởi tạo lại đối tượng.
page_size (tùy chọn): Số lượng dữ liệu trả về trong một lần request. Mặc định là 100. Không giới hạn số lượng tối đa. Tăng số này lên để lấy toàn bộ dữ liêu, ví dụ 10_000.
last_time (tùy chọn): Thời gian cắt dữ liệu, dùng để lấy dữ liệu sau thời gian cắt. Mặc định là None.

Dữ liệu trả về 
<class 'pandas.core.frame.DataFrame'>	
```

## III. Yêu cầu từ người dùng cuối 

Theo dõi giá cổ phiếu theo thời gian thực
Theo dõi khối lượng giao dịch và giá khớp lệnh theo thời gian thực
Từ đó dự đoán xu hướng thị trường, có thể lập quyết định mua hay bán.


Theo dõi giá cổ phiếu trong lịch sử 
Có dữ liệu cơ sở để làm các báo cáo dự báo xu hướng của cổ phiếu đó 

## IV. Schema Design 

![An image for Schema Design .](/github-mark-white.png)

## V. ELT pipeline 
### 1. ELT diagram 
![An image for ELT pipeline diagram.](/github-mark-white.png)
### 2. TaskFlow 
![An image for TaskFlow.](/github-mark-white.png)
