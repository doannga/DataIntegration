import os
import json
import re
career_dict = {
	"hành chính văn phòng": "Hành chính-Văn phòng",
	"hành chính thư ký trợ lý": "Hành chính-Văn phòng",
	"thư ký trợ lý": "Hành chính-Văn phòng",
	"hành chính thư ký": "Hành chính-Văn phòng",
	"hánh chánh thư ký": "Hành chính-Văn phòng",

	"kế toán kiểm toán": "Kế toán - Kiểm toán",
	"Tài chính kế toán kiểm toán": "Kế toán - Kiểm toán",
	"kiểm toán": "Kế toán - Kiểm toán",
	"kế toán": "Kế toán - Kiểm toán",

	"thương mại điện tử": "Thương mại điện tử",
	"internet online media": "Thương mại điện tử",
	"": "Thương mại điện tử",

	"tài chính đầu tư": "Tài chính - Ngân hàng",
	"ngân hàng": "Tài chính - Ngân hàng",
	"đầu tư": "Tài chính - Ngân hàng",
	"ngân hàng tài chính": "Tài chính - Ngân hàng",
	"Ngân hàng chứng khoán đầu tư": "Tài chính - Ngân hàng",

	"quản trị kinh doanh": "Quản trị kinh doanh",
	"kinh tế": "Quản trị kinh doanh",

	"marketing pr": "Marketing - Tiếp thị - Quảng cáo",
	"tiếp thị quảng cáo": "Marketing - Tiếp thị - Quảng cáo",
	"tiếp thị marketing": "Marketing - Tiếp thị - Quảng cáo",
	"tiếp thị trực tuyến": "Marketing - Tiếp thị - Quảng cáo",
	"bán lẻ bán sỉ": "Marketing - Tiếp thị - Quảng cáo",
	"quảng cáo đối ngoại truyền thông": "Marketing - Tiếp thị - Quảng cáo",
	"phát triển thị trường": "Marketing - Tiếp thị - Quảng cáo",
	"promotion girl boy pg pb": "Marketing - Tiếp thị - Quảng cáo",
	"quảng cáo marketing pr": "Marketing - Tiếp thị - Quảng cáo",
	"pg pb lễ tân": "Marketing - Tiếp thị - Quảng cáo",
	"marketing tư vấn": "Marketing - Tiếp thị - Quảng cáo",
	"nghiên cứu thị trường": "Marketing - Tiếp thị - Quảng cáo",
	"quảng cáo khuyến mãi đối ngoại": "Marketing - Tiếp thị - Quảng cáo",
	"marketing": "Marketing - Tiếp thị - Quảng cáo",

	"bảo hiểm": "Bảo hiểm - Chứng khoán",
	"chứng khoán vàng": "Bảo hiểm - Chứng khoán",
	"bảo hiểm tư vấn bảo hiểm": "Bảo hiểm - Chứng khoán",
	"tư vấn bảo hiểm": "Bảo hiểm - Chứng khoán",
	"chứng khoán": "Bảo hiểm - Chứng khoán",

	"nhân sự": "Nhân sự",
	"hành chính nhân sự": "Nhân sự",
	"quản lý điều hành": "Nhân sự",

	"sản xuất vận hành sản xuất": "Sản xuất - Vận hành sản xuất",
	"khu chế xuất khu công nghiệp": "Sản xuất - Vận hành sản xuất",
	"sản xuất ": "Sản xuất - Vận hành sản xuất",

	"quản lý chất lượng qa qc": "Quản lý chất lượng(QA/QC)",
	"qa qc thẩm định giám định": "Quản lý chất lượng(QA/QC)",
	"thẩm định giám định quản lý chất lượng": "Quản lý chất lượng(QA/QC)",
	"quản lý chất lượng": "Quản lý chất lượng(QA/QC)",
	"quảng cáo khuyến mãi": "Quản lý chất lượng(QA/QC)",
	"qa qc": "Quản lý chất lượng(QA/QC)",

	"bảo trì sửa chữa":"Nhân viên Bảo trì - Sửa chữa",

	"nhân viên kinh doanh": "Nhân viên kinh doanh - Bán hàng - Tư  vấn - CSKH",
	"bán hàng": "Nhân viên kinh doanh - Bán hàng - Tư vấn - CSKH",
	"tư vấn": "Nhân viên kinh doanh - Bán hàng - Tư  vấn - CSKH",
	"hàng gia dụng": "Nhân viên kinh doanh - Bán hàng - Tư  vấn - CSKH",
	"bán hàng kinh doanh": "Nhân viên kinh doanh - Bán hàng - Tư  vấn - CSKH",
	"hàng gia dụng chăm sóc cá nhân": "Nhân viên kinh doanh - Bán hàng - Tư  vấn - CSKH",
	"tư vấn": "Nhân viên kinh doanh - Bán hàng - Tư  vấn - CSKH",
	"dịch vụ khách hàng": "Nhân viên kinh doanh - Bán hàng - Tư  vấn - CSKH",
	"tư vấn chăm sóc khách hàng": "Nhân viên kinh doanh - Bán hàng - Tư  vấn - CSKH",
	"chăm sóc khách hàng": "Nhân viên kinh doanh - Bán hàng - Tư  vấn - CSKH",
	"kinh doanh bán hàng": "Nhân viên kinh doanh - Bán hàng - Tư  vấn - CSKH",
	"hàng tiêu dùng": "Nhân viên kinh doanh - Bán hàng - Tư  vấn - CSKH",
	"bán lẻ bán sỉ": "Nhân viên kinh doanh - Bán hàng - Tư  vấn - CSKH",
	"hàng cao cấp": "Nhân viên kinh doanh - Bán hàng - Tư  vấn - CSKH",
	"bán hàng": "Nhân viên kinh doanh - Bán hàng - Tư  vấn - CSKH",
	"dịch vụ khách hàng": "Nhân viên kinh doanh - Bán hàng - Tư  vấn - CSKH",
	"bán hàng kỹ thuật": "Nhân viên kinh doanh - Bán hàng - Tư  vấn - CSKH",

	"dịch vụ": "Dịch vụ - Khách sạn - Nhà hàng - Du lịch",
	"khách sạn nhà hàng": "Dịch vụ - Khách sạn - Nhà hàng - Du lịch",
	"du lịch": "Dịch vụ - Khách sạn - Nhà hàng - Du lịch",
	"nhà hàng khách sạn": "Dịch vụ - Khách sạn - Nhà hàng - Du lịch",
	"du lịch nhà hàng khách sạn": "Dịch vụ - Khách sạn - Nhà hàng - Du lịch",
	"du lịch khách sạn": "Dịch vụ - Khách sạn - Nhà hàng - Du lịch",
	"hàng không du lịch": "Dịch vụ - Khách sạn - Nhà hàng - Du lịch",

	"tổ chức sự kiện quà tặng": "Tổ chức sự kiện - Quà tặng",
	"tổ chức sự kiện": "Tổ chức sự kiện - Quà tặng",

	"KD bất động sản": "Bất động sản",
	"bất động sản": "Bất động sản",

	"ngoại thương xuất nhập khẩu": "Kinh doanh quốc tế",
	"xuất nhập khẩu": "Kinh doanh quốc tế",
	"xuất nhập khẩu ngoại thương": "Kinh doanh quốc tế",
	"ngoại thương xnk": "Kinh doanh quốc tế",

	"hoạch định dự án": "Hoạch định dự án",

	"it phần mềm": "CNTT - Phần mềm",
	"cntt phần mềm": "CNTT - Phần mềm",
	"công nghệ thông tin": "CNTT - Phần mềm",
	"cntt tin học": "CNTT - Phần mềm",

	"it phần cứng mạng": "CNTT - Phần cứng/Mạng",
	"cntt phần cứng mạng": "CNTT - Phần cứng/Mạng",

	"điện tử viễn thông": "Điện tử viễn thông",
	"viễn thông": "Điện tử viễn thông",

	"cơ khí chế tạo": "Cơ khí – Chế tạo – Tự động hoá – Kỹ thuật ứng dụng",
	"ô tô xe máy": "Cơ khí – Chế tạo – Tự động hoá – Kỹ thuật ứng dụng",
	"ô tô   xe máy": "Cơ khí – Chế tạo – Tự động hoá – Kỹ thuật ứng dụng",
	"cơ khí ô tô tự động hoá": "Cơ khí – Chế tạo – Tự động hoá – Kỹ thuật ứng dụng",
	"kỹ thuật ứng dụng": "Cơ khí – Chế tạo – Tự động hoá – Kỹ thuật ứng dụng",
	"cơ khí": "Cơ khí – Chế tạo – Tự động hoá – Kỹ thuật ứng dụng",
	"tự động hoá ô tô": "Cơ khí – Chế tạo – Tự động hoá – Kỹ thuật ứng dụng",

	"điện điện tử điện lạnh": "Điện – điện tử - điện lạnh",
	"điện điện công nghiệp": "Điện – điện tử - điện lạnh",
	"điện tử": "Điện – điện tử - điện lạnh",
	"điện điện tử": "Điện – điện tử - điện lạnh",
	"điện lạnh nhiệt lạnh": "Điện – điện tử - điện lạnh",

	"kỹ thuật": "Kỹ thuật",

	"công nghiệp": "Công nghiệp",

	"in ấn xuất bản": "In ấn – xuất bản",
	"xuất bản in ấn": "In ấn – xuất bản",

	"dầu khí hoá chất": "Dầu khí – Hóa chất – Khoáng sản",
	"dầu khí": "Dầu khí – Hóa chất – Khoáng sản",
	"khoáng sản": "Dầu khí – Hóa chất – Khoáng sản",
	"trắc địa địa chất": "Dầu khí – Hóa chất – Khoáng sản",
	"dầu khí địa chất": "Dầu khí – Hóa chất – Khoáng sản",
	"dầu khí khoáng sản": "Dầu khí – Hóa chất – Khoáng sản",
	"địa chất khoáng sản": "Dầu khí – Hóa chất – Khoáng sản",

	"công nghệ cao": "Công nghệ cao",

	"khoa học tự nhiên": "Khoa học tự nhiên",

	"hàng không": "Hàng không",

	"hàng hải": "Hàng hải",

	"trang thiết bị công nghiệp": "Trang thiết bị gia dụng - công nghiệp - văn phòng",
	"trang thiết bị văn phòng": "Trang thiết bị gia dụng - công nghiệp - văn phòng",
	"trang thiết bị gia dụng": "Trang thiết bị gia dụng - công nghiệp - văn phòng",
	"sản phẩm công nghiệp": "Trang thiết bị gia dụng - công nghiệp - văn phòng",

	"dệt may da giày": "Dệt may - da giày - Thời Trang",
	"dệt may da giày thời trang": "Dệt may - da giày - Thời Trang",
	"thời trang": "Dệt may - da giày - Thời Trang",
	"dệt may": "Dệt may - da giày - Thời Trang",
	"giày da thuộc da": "Dệt may - da giày - Thời Trang",
	"thời trang nghệ thuật": "Dệt may - da giày - Thời Trang",

	"thực phẩm đồ uống": "Thực phẩm – đồ uống",
	"công nghệ thực phẩm dinh dưỡng": "Thực phẩm – đồ uống",
	"thực phẩm dv ăn uống": "Thực phẩm – đồ uống",

	"xây dựng": "Xây dựng",

	"kiến trúc tk nội thất": "Kiến trúc – Thiết kế nội thất",
	"nội ngoại thất": "Kiến trúc – Thiết kế nội thất",
	"đồ gỗ": "Kiến trúc – Thiết kế nội thất",
	"kiến trúc": "Kiến trúc – Thiết kế nội thất",
	"kiến trúc thiết kế nội thất": "Kiến trúc – Thiết kế nội thất",
	"kiến trúc nội thất": "Kiến trúc – Thiết kế nội thất",
	"trang trí nội thất": "Kiến trúc – Thiết kế nội thất",

	"thuỷ lợi": "Giao thông vận tải – Thủy lợi - Cầu đường",
	"giao thông vận tải thuỷ lợi cầu đường": "Giao thông vận tải – Thủy lợi - Cầu đường",

	"thiết kế mỹ thuật": "Thiết kế - Mỹ thuật",
	"mỹ thuật nghệ thuật thiết kế": "Thiết kế - Mỹ thuật",
	"mỹ thuật hội hoạ": "Thiết kế - Mỹ thuật",
	"thiết kế tạo mẫu": "Thiết kế - Mỹ thuật",

	"thiết kế đồ hoạ web": "Thiết kế đồ họa web",

	"thủ công mỹ nghệ": "Thủ công mỹ nghệ",

	"nghệ thuật điện ảnh": "Nghệ thuật – điện ảnh",

	"game": "Giải trí",
	"games": "Giải trí",
	"giải trí": "Giải trí",
	"thể dục thể thao": "Giải trí",
	"giải trí vui chơi": "Giải trí",

	"biên phiên dịch": "Biên – Phiên dịch",
	"phiên dịch ngoại ngữ": "Biên – Phiên dịch",
	"biên dịch phiên dịch": "Biên – Phiên dịch",
	"ngoại ngữ": "Biên – Phiên dịch",
	"ngoại ngữ biên phiên dịch": "Biên – Phiên dịch",

	"báo chí truyền hình": "Báo chí – tuyên truyền",
	"truyền hình báo chí biên tập": "Báo chí – tuyên truyền",
	"biên tập báo chí truyền hình": "Báo chí – tuyên truyền",
	"báo chí biên tập viên": "Báo chí – tuyên truyền",
	"báo chí biên tập": "Báo chí – tuyên truyền",
	"truyền hình truyền thông báo chí": "Báo chí – tuyên truyền",
	"": "Báo chí – tuyên truyền",
	"": "Báo chí – tuyên truyền",

	"quan hệ đối ngoại": "Quan hệ đối ngoại",
	"xuất khẩu lao động": "Quan hệ đối ngoại",
	"đối ngoại": "Quan hệ đối ngoại",
	"quan hệ cộng đồng": "Quan hệ đối ngoại",

	"hóa học sinh học": "Hóa học – Sinh học",
	"hóa học": "Hóa học – Sinh học",
	"công nghệ sinh học": "Hóa học – Sinh học",
	"dược hoá chất sinh hoá": "Hóa học – Sinh học",
	"hoá học hoá sinh": "Hóa học – Sinh học",

	"môi trường": "Môi trường – Xử lý chất thải",
	"môi trường xử lý chất thải": "Môi trường – Xử lý chất thải",

	"y tế chăm sóc sức khoẻ": "Y tế - dược",
	"dược phẩm": "Y tế - dược",
	"y tế dược": "Y tế - dược",
	"y tế": "Y tế - dược",
	"y tế dược phẩm": "Y tế - dược",
	"dược phẩm công nghệ sinh học": "Y tế - dược",
	"bác sĩ": "Y tế - dược",
	"y sĩ hộ lý": "Y tế - dược",
	"dược sĩ": "Y tế - dược",
	"trình dược viên": "Y tế - dược",

	"giáo dục đào tạo": "Sư phạm",
	"thư viện": "Sư phạm",
	"giáo dục đào tạo thư viện": "Sư phạm",

	"nông lâm ngư nghiệp": "Nông - Lâm - Ngư nghiệp",
	"lâm nghiệp": "Nông - Lâm - Ngư nghiệp",
	"nông nghiệp": "Nông - Lâm - Ngư nghiệp",
	"thuỷ sản hải sản": "Nông - Lâm - Ngư nghiệp",
	"chăn nuôi thú y": "Nông - Lâm - Ngư nghiệp",
	"nông lâm": "Nông - Lâm - Ngư nghiệp",
	"nông nghiệp lâm nghiệp": "Nông - Lâm - Ngư nghiệp",

	"an ninh bảo vệ": "An ninh – bảo vệ",
	"bảo vệ an ninh vệ sỹ": "An ninh – bảo vệ",
	"bảo vệ vệ sĩ an ninh": "An ninh – bảo vệ",

	"pháp lý luật": "Luật",
	"luật pháp lý": "Luật",
	"pháp luật pháp lý": "Luật",
	"pháp lý": "Luật",

	"kho vận vật tư": "Bưu chính - Giao vận - Kho vận - Vật tư",
	"thu mua vật tư": "Bưu chính - Giao vận - Kho vận - Vật tư",
	"vận chuyển giao nhận kho vận": "Bưu chính - Giao vận - Kho vận - Vật tư",
	"giao nhận vận chuyển kho bãi": "Bưu chính - Giao vận - Kho vận - Vật tư",
	"vận tải": "Bưu chính - Giao vận - Kho vận - Vật tư",
	"bưu chính viễn thông": "Bưu chính - Giao vận - Kho vận - Vật tư",
	"vật tư thiết bị mua hàng": "Bưu chính - Giao vận - Kho vận - Vật tư",
	"kho vận vật tư thu mua": "Bưu chính - Giao vận - Kho vận - Vật tư",
	"tài xế lái xe giao nhận": "Bưu chính - Giao vận - Kho vận - Vật tư",
	"vận tải lái xe tài xế": "Bưu chính - Giao vận - Kho vận - Vật tư",
	"bưu chính vt": "Bưu chính - Giao vận - Kho vận - Vật tư",
	"vận chuyển giao nhận": "Bưu chính - Giao vận - Kho vận - Vật tư",
	"kho vận": "Bưu chính - Giao vận - Kho vận - Vật tư",
	"thu Mua vật tư cung vận": "Bưu chính - Giao vận - Kho vận - Vật tư",

	"spa mỹ phẩm trang sức": "Spa-Mỹ phẩm-Trang sức",
	"mỹ phẩm trang sức": "Spa-Mỹ phẩm-Trang sức",
	"làm đẹp thể lực spa": "Spa-Mỹ phẩm-Trang sức",
	"mỹ phẩm thời trang trang sức": "Spa-Mỹ phẩm-Trang sức",
	"mỹ phẩm": "Spa-Mỹ phẩm-Trang sức",

	"người giúp việc phục vụ tạp vụ": "Giúp việc - Phục vụ",
	"nhân viên trông quán internet": "Giúp việc - Phục vụ",
	"phục vụ tạp vụ giúp việc": "Giúp việc - Phục vụ",
	
	"phi chính phủ phi lợi nhuận": "Phi chính phủ - Phi lợi nhuận",

	"ngành nghề khác": "Khác",
	"lao động phổ thông": "Khác",
	"ngành khác": "Khác",
	"thống kê": "Khác",
	"freelance": "Khác",
	"làm bán thời gian": "Khác",
	"lương cao": "Khác",
	"công chức viên chức": "Khác",
	"sinh viên làm thêm": "Khác",
	"việc làm tết": "Khác",
	"thời vụ bán thời gian": "Khác",
	"sinh viên mới tốt nghiệp thực tập": "Khác"
}
with open('/home/nga/Documents/Project/DataIntegration/CrawlData/CrawlData/vieclam24h2.json', encoding='utf-8-sig') as f1:
    X = json.loads(f1.read())
    f1.close()

for careers in X:
	ca = []
	careers = careers.get("career")
	for x in careers:
		x = x.lower().replace("-"," ").replace("/"," ")
		career = re.sub("\s\s+" , " ", x)
		# print(career)
		if career in career_dict:
			ca.append(career_dict[career])
			print(career_dict[career])
	care = list(dict.fromkeys(ca))
	print(care)
	print("------------")