import mytools
import mytoken

#https://yandex.ru/dev/direct/doc/reports/fields-list.html

def get_reports():
    type_reports = "CUSTOM_REPORT"

    project = ['хххххх']

    yad_metricks = ["Date",
    "CampaignName",
    "CampaignId",
    "Device",
    "Impressions",
    "Clicks",
    "Cost"]

    DateFrom = '2021-10-01'
    DateTo = '2021-10-10'
    LastDate = 0
    replacement = 'no'

    mytools.yad_mcc(mytoken.mt['i-media'],project,DateFrom,DateTo,yad_metricks,type_reports,LastDate,replacement)

def main():
    get_reports()


if __name__ == '__main__':
    main()
