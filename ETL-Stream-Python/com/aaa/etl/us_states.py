'''
Created on 2024. 12. 21.

@author: pyw20
'''

from enum import Enum

class US_STATES(Enum):
    AK="Alaska"
    AL="Alabama"
    AR="Arkansas"
    AZ="Arizona"
    CA="California"
    CO="Colorado"
    CT="Connecticut"
    #DC="the District of Columbia"
    DE="Delaware"
    FL="Florida"
    GA="Georgia"
    HI="Hawaii"
    IA="Iowa"
    ID="Idaho"
    IL="Illinois"
    IN="Indiana"
    KS="Kansas"
    KY="Kentucky"
    LA="Louisiana"
    MA="Massachusetts"
    MD="Maryland"
    ME="Maine"
    MI="Michigan"
    MN="Minnesota"
    MO="Missouri"
    MS="Mississippi"
    MT="Montana"
    NC="North Carolina"
    ND="North Dakota"
    NE="Nebraska"
    NH="New Hampshire"
    NJ="New Jersey"
    NM="New Mexico"
    NV="Nevada"
    NY="New York"
    OH="Ohio"
    OK="Oklahoma"
    OR="Oregon"
    PA="Pennsylvania"
    RI="Rhode Island"
    SC="South Carolina"
    SD="South Dakota"
    TN="Tennessee"
    TX="Texas"
    UT="Utah"
    VA="Virginia"
    VT="Vermont"
    WA="Washington"
    WI="Wisconsin"
    WV="West Virginia"
    WY="Wyoming"

    @staticmethod
    def parse(input_str):
        list_state = [i.value for i in US_STATES if i.name.lower() == input_str.lower() or i.value.lower() == input_str.lower()]
        if list_state.__len__() == 1:
            return list_state.pop(0)