import pyspark.sql.functions as F
from Framework.spark import spark

try:
    import networkx as nx # type: ignore
except:
    pass
import pyspark.sql.functions as F # type: ignore
import pyspark.sql.window as W # type: ignore
import pyspark.sql.types as T # type: ignore

from pyspark.sql.window import Window # type: ignore

import numpy as np

CRITICAL_EVENTS = [
    
]


def regex_dict (column:str  = "activity"):
    
    regex_dict = {

        'SERVICE_TRANSFER'   : [ 
                                r'Locate&CheckServiceBox-ServiceTransfer',
                                r'SELECT_FLOW-Locate&CheckServiceBox-HowWouldYouLikeToProceedSelection-Talk to a specialist',
                                r'MESSAGE-Locate&CheckServiceBox-TSCantFindSB-ServiceTransfer' ,
                                r'Locate&CheckServiceBox.{0,50}ServiceTransfer',
                                r'SELECT_FLOW-Locate&CheckServiceBox-TSThisIsNotAServiceBox-HowWouldYouLikeToProceedSelection-Talk to a specialist',
                                r'Locate.{5,60}Talk to a specialist',
                                r'Locate.*-ServiceTransfer',
        #                         ],
        # 'S2-SERVICE_TRANSFER'   : [
                                r'API_CALL-GatewayUnboxing&SetUp.{0,10}-ServiceTransfer',
                                r'GatewayUnboxing&SetUp.{0,50}ServiceTransfer',
                                r'.*&Setup.*-Talk to a specialist',
                                ],
        # I SHOULD EXPECT A CALL....






        'S1-START'              : [ 
                                r'MESSAGE-StepsOverview-Next', 
                                r'MESSAGE-StepsOverview$',
                                r'MESSAGE-Locate&CheckServiceBox-Step1-Next$', 
                                r'MESSAGE-Locate&CheckServiceBox-Step1$',
                                r'MESSAGE-Locate&CheckServiceBox-ServiceBoxModels-Next$',  
                                r'WELCOME-WelcomeONT', 
                                r'WELCOME-WelcomeFJ',
                                r'WELCOME-Welcome',
                                r'MESSAGE-StepsOverview-Go Back' , 
                                r'MESSAGE-Locate&CheckServiceBox-Step1-',
                                r'WELCOME-WelcomeInstallation',
                                r'IMAGE_ANALYSIS-WelcomeONT',
                                r'LOGICAL_PROCESSOR-StepsOverview',
                                r'MESSAGE-StepsOverview-(ont|fiber|sb|fj)Installation$',
                                r'MESSAGE-StepsOverview-(ont|fiber|sb|fj)Installation-(Next|Go)',
                                r'MESSAGE-StepsOverview-',
                                r'MESSAGE-StepsOverviewNotRegistered',
                                r'MESSAGE-StepsOverviewRegistered',
                                r'.*Locate.*Step1',
                                ], 

        'S1-INFORMATION'           :[
                                    r'(?i)(?!.*Guestmode.*registered)(MESSAGE|SELECT_FLOW|WELCOME)-.*Locate.*(Classification|Close|Confirmation|Step)',
                                    r'.*Locate.*CheckInstructions',
                                    r'.*Locate.*-ModelSelection$',
                                    r'.*Locate.*-CheckRegistration-Go back',
                                    r'.*Locate.*-ModelCheckProcessor-Next',
                                    r'.*Locate.*-PreSHMHandover'  ,
                                    r'.*Locate.*-DidYouFind.*-Next',
                                    r'.*Locate.*-DidYouFind.*Answer-Next',
        #                         ],
        # 'S1-MODEL_SELECT'       : [ 
                                    r'SELECT_FLOW-Locate&CheckServiceBox-WhatSBModelDoYouHaveSelection', 
                                    r'Locate&CheckServiceBox-ServiceBoxModels-',
                                    r'SELECT_FLOW-Locate&CheckServiceBox-TSPonOff-WhatSBModelDoYouHaveSelection-Model', # " 1" " 2" " 3" ...
                                    r'SELECT_FLOW-Locate&CheckServiceBox-ETH-ModelSelection-Model',
                                ],

        'S1-SKIP-EQUIPMENT'           :[
                                    r'.*Locate.*Skip(Ont){0,1}Check(-Next|-Skip|-Go)',
                                ],

        'S1-LOCATE_EQUIPMENT'  : [ 
                                r'MESSAGE-Locate&CheckServiceBox-ONTLocationKnown', 
                                #    r'Locate&CheckServiceBox-DidYouFind', 
                                r'MESSAGE-Locate.*-Step.*-Next',
                                r'MESSAGE-Locate&CheckServiceBox-HelpFind.{1,5}-(Next|Go|More)', 
                                r'MESSAGE-Locate&CheckServiceBox-ACoupleOptions', 
                                r'MESSAGE-Locate&CheckServiceBox-HelpFind.{1,5}',
                                r'MESSAGE-Locate&CheckServiceBox-AFewSpotsYouCanCheck(-Next){0,1}$',
                                r'MESSAGE-LocateFJ-.{0,5}LocationKnown-', # LivingRoom-Next
                                r'SELECT_FLOW-Locate&CheckServiceBox-DidYouFind(SB|FJ)$',
                                r'MESSAGE-Locate&CheckServiceBox-ETH-LookForONTJack-Next',
                                r'MESSAGE-LocateFJ-ACoupleOptions-More',
                                # r'MESSAGE-Locate&CheckServiceBox-NoCamera-NeededCameraAccess',
                                # r'MESSAGE-LocateFJ-NoCamera-NeededCameraAccess-Next',
                                r'MESSAGE-LocateFJ-NoCamera-NeedFindFJ-Next',
                                # r'MESSAGE-Locate&CheckServiceBox-ETH',
                                # r'LOGICAL_PROCESSOR-Locate.*-DidYouFind(FJ|SB){0,1}Answer$',
                                r'MESSAGE-Locate&CheckServiceBox-TSThisIsNotAServiceBoxOtherDevice$',
                                r'MESSAGE-Locate&CheckServiceBox-TSThisIsNotAServiceBoxOtherDevice-(Next|Go)',
                                r'MESSAGE-Locate&CheckServiceBox-DidYouFindSBRecheck-Go',
                                r'MESSAGE-Locate&CheckServiceBox-ETH-LookForONTJack',
                                r'SELECT_FLOW-Locate&CheckServiceBox-DidYouFindSBRecheck',
                                r'.*Locate.*Skip(Ont){0,1}Check(-Check|$)',
                                r'.*Locate.*DidYouFindSB$',
                                r'.*Locate.*ACoupleOptions$',
                                r'.*Locate.*FJDetached',
                                r'.*Locate.*TipsFindServiceBox',
                                r'.*Locate.*(AFewSpots|HelpFind)',
                                r'.*Locate.*DidYouFind.{0,2}$',
                                r'.*Locate.*-HowWouldYouLikeToProceedSelection',
                                r'.*Locate.*DidYouFind.*Jack$',
                                ],

        'S1-LOCATE_EQUIPMENT_NO'  : [ 
                                    r'MESSAGE-Locate&CheckServiceBox-.{1,2}ThisIsNotAServiceBox',
                                    r'SELECT_FLOW-Locate&CheckServiceBox-DidYouFind.{1,3}-No',
                                    r'SELECT_FLOW-Locate&CheckServiceBox-HowWouldYouLikeToProceedSelection-Try again later',
                                    r'MESSAGE-Locate&CheckServiceBox-TSCantFindSB-TryAgainLaterEnd',
                                    r'MESSAGE-Locate&CheckServiceBox-TryAgainLaterEnd',
                                    r'SELECT_FLOW-Locate&CheckServiceBox-DidYouFindSB-I think I did, but I\'m not sure',
                                    r"SELECT_FLOW-Locate&CheckServiceBox-DidYouFindSBRecheck-I think I did, but I'm not sure",
                                    r'MESSAGE-Locate&CheckServiceBox-NoCamera-DidntFindSB-Next',
                                    r'SELECT_FLOW-LocateFJ-DidYouFindFJ-No',
                                    r'MESSAGE-Locate&CheckServiceBox-NoCamera-DidntFindSB-More options',
                                    r'MESSAGE-LocateFJ-HelpFindFJ-Next',
                                    r'SELECT_FLOW-Locate&CheckServiceBox-TSThisIsNotAServiceBox-HowWouldYouLikeToProceedSelection-I want to keep search',
                                    r'MESSAGE-LocateFJ-TSCantFindFJ-ThisIsNotFiberJack-Next',
                                    r'MESSAGE-LocateFJ-HelpFindFJ-More options',
                                    r'MESSAGE-Locate&CheckFJ-Close',
                                    r'MESSAGE-Locate&CheckServiceBox-NoCamera-ACoupleOptions-Next',
                                    r'MESSAGE-LocateFJ-ACoupleOptions-Next',
                                    r'SELECT_FLOW-Locate&CheckServiceBox-SBNotFound-NoCamera-ManualSBCheckSelection-No',
                                    r'SELECT_FLOW-Locate&CheckServiceBox-NoCamera-HowWouldYouLikeToProceedSelection-Help me find my Service Box',
                                    r'MESSAGE-LocateFJ-TSCantFindFJ-ThisIsNotFiberJack-More options',
                                    r'MESSAGE-LocateFJ-TSCantFindFJ-ThisIsNotFiberJack-RG-Next',
                                    r'MESSAGE-LocateFJ-TSCantFindFJ-CoupleOptionsForYou-Next',
                                    r'SELECT_FLOW-LocateFJ-DidYouFindFJ-I think I did, but I',
                                    r'SELECT_FLOW-Locate&CheckServiceBox-HowWouldYouLikeToProceedSelection',
                                    r'MESSAGE-LocateFJ-AFewSpotsYouCanCheck-Next',
                                    r'SELECT_FLOW-Locate&CheckServiceBox-TSThisIsNotAServiceBox-HowWouldYouLikeToProceedSelection-Try again later',
                                    r'SELECT_FLOW-Locate&CheckServiceBox-HowWouldYouLikeToProceedSelection-I.{1,10}keep search', 
                                    r'MESSAGE-LocateFJ-ACoupleOptions-Go back',
                                    r'MESSAGE-Locate&CheckServiceBox-NoCamera-ACoupleOptions-Go back',
                                    r'MESSAGE-Locate&CheckServiceBox-TipsFindServiceBox-Next',
                                    r'SELECT_FLOW-Locate&CheckServiceBox-TSThisIsNotAServiceBox-',
                                    r'SELECT_FLOW-LocateFJ-TSCantFindFJ',
                                    r'IMAGE_ANALYSIS-Locate&CheckServiceBox-NoCamera-DidntFindSB',
                                    r'IMAGE_ANALYSIS-Locate&CheckServiceBox-SBNotFound-NoCamera-ManualSBCheckSelection',
                                    r'IMAGE_ANALYSIS-Locate&CheckServiceBox-TSThisIsNotAServiceBox',        
                                    r"SELECT_FLOW-Locate&CheckServiceBox-ACoupleOptions-No, I didn't",
                                    r'SELECT_FLOW-Locate&CheckServiceBox-CheckDD-No',
                                    r'.*Locate.*-TSCantFind(FJ|SB)',
                                    r'.*Locate.*SBNotFound(?<!-Yes)$',
                                    r'.*Locate.*DidYouFind.*-No',
                                    ],

        'S1-LOCATE_EQUIPMENT_YES'  : [ 
                                    r'SELECT_FLOW-Locate&CheckServiceBox-SBNotFound-NoCamera-ManualSBCheckSelection-Yes',
                                    r'MESSAGE-Locate&CheckServiceBox-SkipOntCheck-Continue', 
                                    r'MESSAGE-Locate&CheckServiceBox-SkipOntCheck-Check AT&T Service Box',
                                    r'SELECT_FLOW-Locate&CheckServiceBox-DidYouFind.{1,3}-Yes',
                                    r'SELECT_FLOW-LocateFJ-DidYouFindFJ-Yes',
                                    r'MESSAGE-LocateFJ-HappyPath-FJFound-Next',
                                    r'SELECT_FLOW-Locate&CheckServiceBox-ETH-DidYouFindONTJack-Yes',
                                    r'MESSAGE-Locate&CheckServiceBox-ETH-ONTJackFound-Model.{1,3}-Next',
                                    r'SELECT_FLOW-Locate&CheckServiceBox-SBNotFound-NoCamera-ManualSBCheckSelection-Yes',
                                    r'SELECT_FLOW-Locate&CheckServiceBox-ACoupleOptions-Yes, I did',
                                    r'MESSAGE-Locate&CheckServiceBox-ETH-ONTJackFound',
                                    # r'IMAGE_ANALYSIS-Locate.*-HappyPath-.{0,2}Found',
                                    r'.*Locate.*(FJ|SB)Found.*(FJ|SB)*',
                                    r'.*Locate.*DidYouFind.*-Yes',
                                    ],
        
        'S1-LED_CHECK'          : [ 
                                r'MESSAGE-Locate&CheckServiceBox-NoCamera-NeededCameraAccess-Go back',
                                r'MESSAGE-Locate&CheckServiceBox-NoCamera-NeededCameraAccess-Self-check',
                                r'MESSAGE-Locate&CheckServiceBox-NoCamera-LEDManualCheck-Next',
                                r'MESSAGE-Locate&CheckServiceBox-NoCamera-LEDManualCheck-Go back',
                                r'MESSAGE-Locate&CheckServiceBox-TSONTPonOff-NoCamera-LEDManualCheck-Next',
                                r'SELECT_FLOW-Locate&CheckServiceBox-TSPonOff-TryAgainSelection-No',
                                r'MESSAGE-Locate&CheckServiceBox-NoCamera-LEDManualCheck',
                                r'SELECT_FLOW-Locate&CheckServiceBox-NoCamera-ManualLEDCheck$',
                                r'MESSAGE-Locate&CheckServiceBox-ONTLEDCheck',
                                r'SELECT_FLOW-Locate&CheckServiceBox-TryAgainONTLEDCheckSelection',
                                r'SELECT_FLOW-LocateFJ-TryAgainONTLEDCheckSelection',
                                ],

        'S1-LED_AI'     : [ 
                        r'AR_MULTI_ELEMENT_GUIDANCE-Locate&CheckServiceBox.{1,50}OffARGuidance',
                        r'AR_MULTI_ELEMENT_GUIDANCE-Locate&CheckServiceBox.{1,50}OnARGuidance',
                        r'AR_MULTI_ELEMENT_GUIDANCE-Locate&CheckServiceBox-TSONTPowerOff-SuccessARGuidance',
                        r'AR_MULTI_ELEMENT_GUIDANCE-Locate&CheckServiceBox-TSPonOff-SuccessARGuidance',
                        r'Locate&CheckServiceBox-LEDCheckProcessor',
                        r'LOGICAL_PROCESSOR-Locate&CheckServiceBox-TSExtraPowerCableSB-LEDRecheckProcessor',
                        r'LOGICAL_PROCESSOR-Locate&CheckServiceBox-TSONTPowerOff-LEDCheckProcessor',
                        r'IMAGE_ANALYSIS-Locate&CheckServiceBox-CheckDevice',
                        r'TAKE_IMAGE-Locate&CheckServiceBox-LEDCheckInstructions$',
                        r'IMAGE_ANALYSIS.*Locate.*LEDCheck$', 
                        r'IMAGE_ANALYSIS-Locate&CheckServiceBox-LEDCheckInstructions',
                        r'IMAGE_ANALYSIS-Locate&CheckServiceBox-NoCamera-LEDManualCheck',
                        r'IMAGE_ANALYSIS-Locate&CheckServiceBox-NoCamera-NeededCameraAccess',
                        r'IMAGE_ANALYSIS-Locate&CheckServiceBox-NoCamera-ManualLEDCheck',
                        r'IMAGE_ANALYSIS-Locate&CheckServiceBox-PonOffARGuidance',
                        r'IMAGE_ANALYSIS-Locate&CheckServiceBox-PowerOffARGuidance',
                        r'IMAGE_ANALYSIS-Locate&CheckServiceBox-RetakePhoto',
                        r'IMAGE_ANALYSIS-Locate&CheckServiceBox-TSExtraPowerCableSB-ONTLEDRecheck',
                        r'IMAGE_ANALYSIS-Locate&CheckServiceBox-.{1,20}Off',
                        r'IMAGE_ANALYSIS-Locate&CheckServiceBox-.{1,20}On',
                        r'IMAGE_ANALYSIS-Locate&CheckServiceBox-TSExtraPowerCableSB-TSPonOffRecheck-UncoveredWithFJ-CheckGreenCable',
                        r'IMAGE_ANALYSIS-Locate&CheckServiceBox-TSExtraPowerCableSB-TryAgainTSONTPonOff',
                        r'IMAGE_ANALYSIS-Locate&CheckServiceBox-TSExtraPowerCableSB',
                        r'TAKE_IMAGE-Locate',
                        r'(IMAGE_ANALYSIS|LOGICAL_PROCESSOR).*Locate.*-TS.*ONTPonOff',
                        r'(IMAGE_ANALYSIS|LOGICAL_PROCESSOR).*Locate.*-TS.*PonOff',
                            ],
        
        'S1-LED_ERROR'          : [r'SELECT_FLOW-Locate&CheckServiceBox-NoCamera-ManualLEDCheck-PON light is off',
                                r'SELECT_FLOW-Locate&CheckServiceBox-NoCamera-ManualLEDCheck-Both lights are off',
                                r'SELECT_FLOW-Locate&CheckServiceBox-TSPonOff-TryAgainSelection-Yes',
                                r'SELECT_FLOW-Locate&CheckServiceBox-TSONTPonOff-NoCamera-ManualLEDCheck-PON light is off',
                                r'SELECT_FLOW-Locate&CheckServiceBox-.{15,80}-PON light is of',
                                r'SELECT_FLOW-Locate&CheckServiceBox-TryAgainONTLEDCheckSelection-No',
                                r'SELECT_FLOW-LocateFJ-TryAgainONTLEDCheckSelection-No',
                                r'.*Locate.*Failed.*LEDCheck',
                                r'SELECT_FLOW-check.*Light.*No',
                                    ],

        'S1-LED_OK'          : [ 
                                r'SELECT_FLOW-Locate&CheckServiceBox-NoCamera-ManualLEDCheck-.{1,15}(Ok|on|correct|working)',
                                r'SELECT_FLOW-Locate&CheckServiceBox-TryAgainONTLEDCheckSelection-Yes',
                                r'SELECT_FLOW-LocateFJ-TryAgainONTLEDCheckSelection-Yes',
                                r'SELECT_FLOW-check.*Light.*Yes',
                                r'SELECT_FLOW-Locate&CheckServiceBox-ONTLEDCheck-Yes',
                                    ],
        
        'S1-CABLE_PLUG'        : [
                                r'MESSAGE-PLocate&CheckServiceBox-TSExtraPowerCableSB-Covered-PlugExtraPowerCable-Next',
                                r'MESSAGE-Locate&CheckServiceBox-TSONTPowerOff-UncoveredWithFJ-PlugPowerCableIntoWorkingPowerOutlet-Next',
                                r'MESSAGE-Locate&CheckServiceBox-TSONTPowerOff-Covered-PlugPowerCableIntoWorkingPowerOutlet-Next',
                                r'MESSAGE-Locate&CheckServiceBox-TSONTPowerOff-UncoveredNoFJ-PlugPowerCableIntoWorkingPowerOutlet-Next',
                                r'MESSAGE-Locate&CheckServiceBox-ETH-PlugONTCableIntoPortModel.{0,5}-Next',
                                r'MESSAGE-Locate&CheckServiceBox-TSPowerOff-Covered-RemoveCover-Next',
                                r'MESSAGE-Locate&CheckServiceBox-TSONTPowerOff-Covered-RemoveCover-Next' ,
                                r'MESSAGE-Locate&CheckServiceBox-TSPonOff-Covered-RemoveCover-Next',
                                r'MESSAGE-Locate&CheckServiceBox-ETH-PlugONTCableIntoPortModel',
        #                         ],
        # 'S1-FIND_CABLES'        : [ 
                                r'SELECT_FLOW-Locate&CheckServiceBox-.{1,20}PowerOff-DoYouHavePowerCableUncoveredWith.{1,5}Selection',
                                r'MESSAGE-Locate&CheckServiceBox-TSExtraPowerCableSB-ExtraPowerCable-Next',
                                r'MESSAGE-Locate&CheckServiceBox-TSPonOff-UncoveredWithFJ-GreenCableCheck-Next',
                                r'MESSAGE-Locate&CheckServiceBox-TSPonOff-UncoveredNoFJ-GreenCableCheck-Next',
                                r'MESSAGE-Locate&CheckServiceBox-TSPonOff-Covered-GreenCableCheck-Next',
                                r'MESSAGE-Locate&CheckServiceBox-TSPonOff-RetakePhoto-Next',
                                r'MESSAGE-Locate&CheckServiceBox-TSONTPonOff-NoCamera-NeededCameraAccess-Self-check',
                                r'MESSAGE-Locate&CheckServiceBox-TSONTPonOffRecheck-NoCamera-NeededCameraAccess-Self-check',
                                r'MESSAGE-Locate&CheckServiceBox-TSONTPonOffRecheck-NoCamera-LEDManualCheck-Next',
                                r'MESSAGE-Locate&CheckServiceBox-RetakePhoto-Next',
                                r'MESSAGE-Locate&CheckServiceBox-TSONTPowerOff-RetakePhoto-Next',
                                r'MESSAGE-Locate&CheckServiceBox-TSONTPowerOff-UncoveredWithFJ-PowerButtonPressed-Next',
                                r'MESSAGE-Locate&CheckServiceBox-TSExtraPowerCableSB-Covered-PowerButtonPressed-Next',
                                r'MESSAGE-Locate&CheckServiceBox-TSONTPowerOff-Covered-PowerButtonPressed-Next',
                                r'MESSAGE-Locate&CheckServiceBox-TSExtraPowerCableSB-UncoveredNo.{0,5}',
                                r'MESSAGE-Locate&CheckServiceBox-TSExtraPowerCableSB-Uncovered(No|With)FJ',
                                r'MESSAGE-Locate&CheckServiceBox-TSExtraPowerCableSB-(TSPonOffRecheck|Covered).{1,40}-Next',
                                r'MESSAGE-Locate&CheckServiceBox-TSONTPowerOff-UncoveredNoFJ-PowerButtonPressed-Next',
                                r'MESSAGE-Locate&CheckServiceBox-TSExtraPowerCableSB-TSONTPonOffPreSHMHandover',
                                r'SELECT_FLOW-Locate&CheckServiceBox-TSONTPowerOff-TryAgainSelection-Yes',
                                r'MESSAGE-Locate&CheckServiceBox-TSExtraPowerCableSB-LookForLooseCables-Next',
                                r'SELECT_FLOW-Locate&CheckServiceBox-TSExtraPowerCableSB-TryAgainTSONTPonOff-Yes',
                                r'.*Locate.*-TS.*Cable',
                                ],

        'S1_CABLES_YES'        : [ r'SELECT_FLOW-Locate&CheckServiceBox-TSExtraPowerCableSB-DidYouFindTheCableSelection-Yes',
                                    r'SELECT_FLOW-Locate&CheckServiceBox-TSONTPowerOff-DoYouHavePowerCableCoveredSelection-Yes',
                                    r'SELECT_FLOW-Locate&CheckServiceBox-TSONTPowerOff-DoYouHavePowerCableUncoveredNoFJSelection-Yes',
                                    ],

        'S1-CABLES_NO'         : [ r'MESSAGE-Locate&CheckServiceBox-TSONTPowerOff-PowerCableToBeSend-Next',
                                    r'SELECT_FLOW-Locate&CheckServiceBox-TSONTPowerOff-DoYouHavePowerCableCoveredSelection-No',
                                    r'SELECT_FLOW-Locate&CheckServiceBox-TSONTPowerOff-DoYouHavePowerCableUncoveredNoFJSelection-No',
                                    r'SELECT_FLOW-Locate&CheckServiceBox-TSExtraPowerCableSB-DidYouFindTheCableSelection-No',
                                    r'SELECT_FLOW-Locate&CheckServiceBox-TSExtraPowerCableSB-DidYouFindExtraCableInTheBoxSelection-No, I didn\'t find the cable',
                                    ],

        # 'S1-SB_NOT_WORKING'     : [ 
        #                             ],

        # 'S1-WORKING'            : [ 
        #                         ],
                                
        # 'S1-API_CONNECT'           : [ 
        #                         ],

        'S1-CHECK_REGISTRATION'    : [ 
                                r'LOGICAL_PROCESSOR-Locate&CheckServiceBox-CheckRegistration$',
                                r"SELECT_FLOW-Locate&Check.*-CheckRegistration-I think I did",
                                r'.*Locate.*-CheckRegistration-No',
                                r'SELECT_FLOW-Locate&Check.*-CheckRegistration-Yes, I did',
                                r'SELECT_FLOW-Locate&CheckServiceBox-CheckDD-I think I did, but ',
                                ],

        'S1-ERROR' :            [
                                r'Locate&CheckServiceBox-.{1,20}PowerOff-Found(SB|FJ)NotWorking',
                                r'MESSAGE-Locate&CheckServiceBox-TSPonOff-Found(SB|FJ)NotWorking',
                                r'.*Locate.*TryAgainLaterEnd',
        #                         ],
        # 'S1-TS-ONT-PON-OFF':    [
                                    r'.*Locate.*-TS.*ONTPonOff',
                                    r'.*Locate.*-TS.*PonOff',
        #                         ],
        # 'S1-TS-ONT-PowerOFF':   [
                                    r'.*Locate.*-TS.*PowerOff',
                            ],

        'S1-FINISH'             : [
                                r'IMAGE_ANALYSIS-Locate&CheckServiceBox-EndMessage',
                                r'IMAGE_ANALYSIS-Locate&CheckServiceBox-HappyPath-ONTIsWorking',
                                r'IMAGE_ANALYSIS-Locate&CheckServiceBox-SuccessARGuidance',
                                r'AR_MULTI_ELEMENT_GUIDANCE-Locate&CheckServiceBox-SuccessARGuidance',
                                r'MESSAGE-Locate&CheckServiceBox-.{1,20}-ONTIsWorking',
                                r'SELECT_FLOW-Locate&CheckServiceBox-NoCamera-ManualLEDCheck-Both lights are green',
                                r'MESSAGE-Locate&CheckServiceBox-EndMessage-Finish', 
                                r'MESSAGE-Locate&CheckServiceBox-Step1-Step Completed',
                                r'MESSAGE-Locate.*-AuthenticatedRegistered',
                                r'MESSAGE-Locate.*-CompleteYourInstallation',
                                r'.*Locate.*-GuestModeNotRegistered-Step',
                                r'.*Locate.*-GuestModeRegistered-End' ,
                                r'.*Locate.*-EndMessage',
                                r'.*Locate.*-AuthenticatedNotRegistered-Step',
                                r'.*Locate.*-AuthenticatedRegistered-End',
                                r'.*Locate.*-Registered-End',
                                ],
        










        # ----------------------------------------------------------------------------------------------------

        'S2-START'              : [ 
                                    r'MESSAGE-AccountSetUp-Step2',
                                    r'MESSAGE-GatewayUnboxing&SetUp-Step2-Next', 
                                    r'MESSAGE-GatewayUnboxing&SetUp-BeCloseToServiceBox-Next', 
                                    r'MESSAGE-GatewayUnboxing&SetUp-BeCloseToServiceBox-Go back',
                                    r'MESSAGE-GatewayUnboxing&SetUp-BeCloseToServiceBox',
                                    r'MESSAGE-GatewayUnboxing&SetUp-OpenBox-Next', 
                                    r'MESSAGE-GatewayUnboxing&SetUp-OpenBox-Go back' , 
                                    r'MESSAGE-GatewayUnboxing&SetUp-Step1-Next',
                                    r'MESSAGE-GatewayUnboxing&SetUp-FrontxBack-Next', 
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-AreYouCloseToServiceBox-Yes',
                                    r'MESSAGE-GatewayUnboxing&SetUp-BeCloseToETHJackModel(1|2|3|\d)',
                                    r'MESSAGE-GatewayUnboxing&SetUp-BeCloseToFJLocation',
                                    r'MESSAGE-GatewayUnboxing&SetUp-Step2',
                                    r'MESSAGE-GatewayUnboxing&SetUp-Step1-Go back',
                                    r'SELECT_FLOW-CheckIfHasReceivedRG-Selection',
                                    r'MESSAGE-GatewayUnboxing&SetUp-Step',
                                    r'.*&SetUp-AreYouClose',
                                    r'.*&SetUp.*OpenBox',
                                    r'.*&SetUp-PreSHMHandover',
                                    r'.*SetUp-VOIP-Step3-Next', 
                                ],

        'S2-SETUP-VOIP'         : [
                                    r'.*SetUpVOIP',
                                    r'MESSAGE-GatewayUnboxing&SetUp-VOIP',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-VOIP-FindPhoneCable$',
                                ],
        'S2-SETUP-VOIP-NO'         : [
                                    r'.*-GatewayUnboxing&SetUp.*VOIP.*-No',
                                ],
        'S2-SETUP-VOIP-YES'         : [
                                    r'.*-GatewayUnboxing&SetUp.*VOIP.*-Yes',
                                ],

        'S2-TRY-AGAIN'          :[
                                    r'.*GatewayUnboxing&SetUp.*TryAgain',
                                ],

        'S2-REBOOT'             :[
                                r'MESSAGE-GatewayUnboxing&SetUp-TSBlinkingRed-RebootInstructions-Next',
                                r'MESSAGE-GatewayUnboxing&SetUp-TSBlinkingRed-ReconnectPowerCable-Next',
                                r'MESSAGE-GatewayUnboxing&SetUp-TSExtraPowerCable-SBPowerOff-UncoveredWithFJ-PowerButtonPressed',
                                r'.*&SetUp-.*-(Reconnect|Reboot)(-Yes|-Next|-Go){0,1}',
                                ],

        'S2-LOCATE_EQUIPMENT_YES' : [ 
                                    r'MESSAGE-GatewayUnboxing&SetUp-.{1,5}IsWorking(-Next|-Go|$)',
                                    r'MESSAGE-GatewayUnboxing&SetUp-TSExtraPowerCable-ONTIsWorking',
                                ],

        'S2-LOCATE_EQUIPMENT_NO'    : [ 
                                    r"SELECT_FLOW-GatewayUnboxing&SetUp-SBHowWouldYouLikeToProceed$", 
                                    r"SELECT_FLOW-GatewayUnboxing&SetUp-SBHowWouldYouLikeToProceed-I.*keep searching",
                                    r"SELECT_FLOW-GatewayUnboxing&SetUp-SBHowWouldYouLikeToProceed-Try again.*",
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-AreYouCloseToServiceBox-I can\'t find the Service Box in my house',
                                    r'.*GatewayUnboxing&SetUp.*(SB|FJ)ThisIsNotA(SB|FJ)',
                                    ],

        'S2-INFORMATION'        : [ 
                                    r'MESSAGE-GatewayUnboxing&SetUp-EducationalMessage',
                                    r'MESSAGE-GatewayUnboxing&SetUp-FrontxBack',
                                    r'MESSAGE-GatewayUnboxing&SetUp-NowLetsSetUpYourDevice',
                                    r'MESSAGE-GatewayUnboxing&SetUp-OpenBox',
                                    r'MESSAGE-GatewayUnboxing&SetUp-TSOtherColors-NeedCameraAccess-Go back',
                                    r'MESSAGE-GatewayUnboxing&SetUp-SB',
                                    r'MESSAGE-GatewayUnboxing&SetUp-ManualLEDCheck',
                                    r'MESSAGE-GatewayUnboxing&SetUp-LEDCheck',
                                    r'MESSAGE-GatewayUnboxing&SetUp-Check',
                                    r'MESSAGE-GatewayUnboxing&SetUp-.*CloseTo',
                                    r'MESSAGE-GatewayUnboxing&SetUp-.*Detached',
                                    # r'MESSAGE-GatewayUnboxing&SetUp-TSOtherColors',
                                    r'.*&SetUp-TakeAPIcture',
                                    r'.*&SetUp-RGModel',
                                    r'.*&SetUp.*CablesConnected-TryAgain.*',
                                    r'.*&SetUp.*-ManualLEDCheck$',
                                    r'.*&SetUp.*-SelectFrontColor',
                                    r'.*&SetUp.*-ProceedSelection',
                                    r'.*&SetUp.*-ModelSelection',
                                    # r'.*&SetUp.*-SelectFrontColor',
                                    r'.*&SetUp.*-DidYouFindThe.*CableSelection$',
                                    r'.*&SetUp.*-DidYouFindTheExtraCableInTheBoxSelection$',
                                    r'.*&SetUp.*-PowerOutletCheckPowerGreenLightSelection$',
                                    r'.*&SetUp.*-CheckPowerGreenLightSelection$',
                                    r'.*&SetUp.*PowerCableRecheckSelection$',
                                    r'MESSAGE-.*&SetUp-.*-ConnectFJ',
                                    r"SELECT_FLOW-CheckCustomerStatus-LogicalProcessor-I think I did, but I'm not sure",
                                    r"SELECT_FLOW-CustomerStatus-SelectFlow-iont-fiber-jack" ,
                                    r'SELECT_FLOW-CustomerStatus-SelectFlow-iont-fiber-jack-installation' ,
                                    r'SELECT_FLOW-CustomerStatus-SelectFlow-iont-installation',
                                    r'MESSAGE-GatewayUnboxing&SetUp-.*CablesCheckSuccess-Next',
                                    r'MESSAGE-GatewayUnboxing&SetUp-.*CablesCheckSuccess-Go back', 
                                    r'MESSAGE-GatewayUnboxing&SetUp-H500CablesCheckSuccess-Next', 
                                    ],
        
        'S2-CABLE_CHECK'        : [ 
                                    r'MESSAGE-GatewayUnboxing&SetUp-.{1,20}CableCheckStart(-Next|-Go|$)', 
                                    r'MESSAGE-GatewayUnboxing&SetUp-.{1,20}CableCheckLogicalProcessor(-Next|-Go|$)',
                                    r'MESSAGE-GatewayUnboxing&SetUp-.{1,20}Check.{1,5}Cable(-Next|-Go|$)',
                                    r'MESSAGE-GatewayUnboxing&SetUp-505-ConnectFJ',
                                    r'MESSAGE-GatewayUnboxing&SetUp-TSExtraPowerCable-SBPonOff-UncoveredWithFJ-CheckGreenCable',
                                    r'MESSAGE-GatewayUnboxing&SetUp-TSExtraPowerCable-SBPowerOff-LookForLooseCables',
                                    r'MESSAGE-GatewayUnboxing&SetUp-TSNoONTCable-LetsContinue-Next',
                                    r'MESSAGE-GatewayUnboxing&SetUp-TSNoONTCable-LookForLooseCable',
                                    r'MESSAGE-GatewayUnboxing&SetUp-TSNoONTCable-NoProblemUseOptionalCables',
                                    r'MESSAGE-GatewayUnboxing&SetUp-TS500CablesNotConnected-ONTNotConnected-MakeSureONTCableIsConnected-Skip',
                                    r'MESSAGE-GatewayUnboxing&SetUp-TS505CablesNotConnected-MakeSurePowerCableIsConnected-Skip',
                                    r'MESSAGE-GatewayUnboxing&SetUp-TS505CablesNotConnected-ONTNotConnected-MakeSureONTCableIsConnected(-Next|-Skip)',
                                    # r'MESSAGE-GatewayUnboxing&SetUp.{1,20}Cable',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-H500CheckONTCable$',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-CheckFiberCable$',
                                    r'.*&SetUp-.*CheckPowerCable$',
                                    r'.*&SetUp-TryAgain',
                                    r'.*&SetUp-.*PowerCable-.*DidYouFindTheCableSelection$',
                                    r'.*&SetUp-.*NoPowerCable.*-DidYouFind.*CableSelection$',
                                    r'MESSAGE-GatewayUnboxing&SetUp.*CableCheckStart$',
                                    r'.*&SetUp.*Covered-Check.*Cable',
                                    r'.*&SetUp.*-ExtraPowerCable$',
                                    r'.*&SetUp.*-ExtraPowerCable-Next',
                                    r'.*&SetUp.*-LookForLooseCables-Next',
                                    r'.*&SetUp.*-PowerButtonPressed-Next',
                                    r'.*&SetUp.*-Uncovered.*-PowerButtonPressed-Next',
                                    r'.*&SetUp.*-UncoveredWith.*-PowerButtonPressed-Next',
                                    r'.*&SetUp.*-UncoveredWith.*-Check.*Cable-Next',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSNoFiberCable-DidYouFindFiberCableSelection',
                                    r'MESSAGE-GatewayUnboxing&SetUp-TSNoONTCable-LetsContinueYellowCableNotice$',
        #                             ],
        # 'S2-CABLE_CHECK_TSNoONTCable' : [
                                    r'MESSAGE-GatewayUnboxing&SetUp-TSNoONTCable-NoProblemUseOptionalCables-Next' ,
                                    r'MESSAGE-GatewayUnboxing&SetUp-TSNoONTCable-LetsContinueYellowCableNotice-Next',
                                    ],

        # 'S2-CABLE_CHECK_AI'     : [ 
        #                             r'LOGICAL_PROCESSOR-GatewayUnboxing&SetUp-.{1,8}CableCheckLogicalProcessor',
        #                             ],

        'S2-CABLE_FIBER_YES'                  : [ 
                                                r'.*&SetUp.*Check.*FiberCable-Yes',
                                                r'SELECT_FLOW-GatewayUnboxing&SetUp-Check(Fiber)Cable-Yes',
                                                r'SELECT_FLOW-GatewayUnboxing&SetUp-TSNoFiberCable-DidYouFindFiberCableSelection-Yes',
                                            ],

        'S2-CABLE_FIBER_NO'                  : [ 
                                                r'.*&SetUp.*Check.*FiberCable-No',
                                            ],

        'S2-CABLE_EQUIPMENT_YES'                  : [ 
                                                r'.*&SetUp-.{1,8}Check.*(ONT|FJ).*Cable-Yes', 
        #                                     ],
        # 'S2-CABLE_ONT_YES_TSNoONTCable'      : [ 
                                                r'SELECT_FLOW-GatewayUnboxing&SetUp-TSNoONTCable-H500PowerCableRecheckSelection-Yes',
                                                r'SELECT_FLOW-GatewayUnboxing&SetUp-TSNoONTCable-DidYouFindTheExtraCableInTheBoxSelection-Yes',
                                                r'MESSAGE-GatewayUnboxing&SetUp-TSNoONTCable-H500CablesCheckSuccess-Next',
                                                r'SELECT_FLOW-GatewayUnboxing&SetUp-TSNoONTCable-.{15,60}-Yes',
                                                r'.*&SetUp-.*Check.*(ONT|FJ).*Cable-Yes',
                                            ],

        'S2-CABLE_EQUIPMENT_NO'       : [ 
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-.{1,8}CheckONTCable-No', 
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSNoFiberCable.*-No',
                                    r'API_CALL-APIGatewayUnboxing&SetUp-TSNoFiberCable.*-No',
                                    r'MESSAGE-GatewayUnboxing&SetUp-TS505CablesNotConnected-ONTNotConnected-Next',
                                    r'MESSAGE-GatewayUnboxing&SetUp-TS500CablesNotConnected-ONTNotConnected(-Next|-Go|$)',
                                    r'MESSAGE-GatewayUnboxing&SetUp-TS500CablesNotConnected-ONTNotConnected-MakeSureONTCableIsConnected(-Next|-Go|$)',
        #                         ],
        # 'S2-CABLE_ONT_NO_TSNoONTCable'   : [ 
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSNoONTCable-DidYouFindTheCableSelection-No', 
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSNoONTCable-DidYouFindTheExtraCableInTheBoxSelection-No',
                                    r'MESSAGE-GatewayUnboxing&SetUp-TSNoONTCable-LookForLooseCable-Next',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSNoONTCable-.{15,60}-No',
                                    r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-TS500CablesNotConnected-ONTNotConnected-InstallationAnalysis',
                                ],

        'S2-CABLE_PLUG'         : [
                                r'MESSAGE-GatewayUnboxing&SetUp-Default-H500PlugONTCableToSB-Next', 
                                r'MESSAGE-GatewayUnboxing&SetUp-UncoveredWithFJ-H500PlugONTCableToSB-Go back',
                                r'MESSAGE-GatewayUnboxing&SetUp-H500PlugONTCableToRG-Next', 
                                r'MESSAGE-GatewayUnboxing&SetUp-TSNoONTCable-H500PlugExtraONTCableToRG-Next',
                                r'MESSAGE-GatewayUnboxing&SetUp-TSNoONTCable-Default-H500PlugExtraONTCableToSB-Next',
                                r'MESSAGE-GatewayUnboxing&SetUp-H500PlugPowerCableIntoRG-Next', 
                                r'MESSAGE-GatewayUnboxing&SetUp-TSNoONTCable-H500PlugPowerCableIntoRG-Next',
                                r'MESSAGE-GatewayUnboxing&SetUp-Covered-RemoveCover',
                                r'MESSAGE-GatewayUnboxing&SetUp-.{1,25}Remove.*Cap',
                                r'MESSAGE-GatewayUnboxing&SetUp-.*Plug.*Cable',
                                r'.*&SetUp.*-Covered-RemoveCover',
                                r'.*&SetUp.*-LookForLooseCablesPower',
                                r'MESSAGE-GatewayUnboxing&SetUp-TSExtraPowerCable-SBPowerOff-PowerButtonPressed',
                                ],

        'S2-CABLE_POWER_YES'    : [ 
                                r'SELECT_FLOW-GatewayUnboxing&SetUp-.{1,8}CheckPowerCable-Yes', 
                                r'MESSAGE-GatewayUnboxing&SetUp-50.{1,25}(Power)Cable',
                                r'SELECT_FLOW-GatewayUnboxing&SetUp-TSLightsOff-PowerOutletCheckPowerGreenLightSelection-Yes',
                                r'.*&SetUp-.*PowerCable-.*DidYouFindTheCableSelection-Yes',
                                r'.*&SetUp-.*NoPowerCable.*-DidYouFind.*CableSelection-Yes',
                                r'.*&SetUp-.*PowerCable.*-DidYouFindTheExtraCableInTheBoxSelection-Yes',
                                r'SELECT_FLOW-GatewayUnboxing&SetUp-Check.*Power.*Cable-Yes',
                                ],

        'S2-CABLE_POWER_NO'     : [ 
                                r'SELECT_FLOW-GatewayUnboxing&SetUp-.{1,8}CheckPowerCable-No',
                                r'SELECT_FLOW-GatewayUnboxing&SetUp-TSLightsOff-CheckPowerGreenLightSelection-No',
                                r'MESSAGE-GatewayUnboxing&SetUp-TS.{1,4}CablesNotConnected-MakeSurePowerCableIsConnected(-Next|-Go)',
                                r'MESSAGE-GatewayUnboxing&SetUp-TS.{1,4}CablesNotConnected-PowerSupplyNotConnected(-Next|-Go)',
                                r'.*&SetUp-CheckPowerCable-No',
                                r'.*&SetUp-.*PowerCable-.*DidYouFindTheCableSelection-No',
                                r'.*&SetUp-.*PowerCable.*-DidYouFind.*CableSelection-No',
                                r'.*&SetUp-.*PowerCable.*-DidYouFindTheExtraCableInTheBoxSelection-No',
                                r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-TS500CablesNotConnected-PowerNotConnected-InstallationAnalysis',
                                # r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp.*-BGW320500-InstallationAnalysis',
                                r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp.*(Cable|Power).*-InstallationAnalysis',
                                r'MESSAGE-AccountSetUpNoPoweCable-NotRegisteredStep2',
                                ],

        'S2-CABLE_OK'      : [ 
                                # r'MESSAGE-GatewayUnboxing&SetUp-CablesCheckSuccess',
                                r'MESSAGE-GatewayUnboxing&SetUp-.{1,15}CablesConnectedCorrectly-Next',
                                r'API_CALL-.{0,8}GatewayUnboxing&SetUp-AllCablesConnected-GetToken',
                                r'API_CALL-.{0,8}GatewayUnboxing&SetUp-AllCablesConnected-{1,3}SendFeedback',
                                r'MESSAGE-GatewayUnboxing&SetUp.{1,20}Cable.*ConnectedCorrectly',
                                r'.*&Setup.*CablesCheckSuccess-Yes',
                                r'MESSAGE-GatewayUnboxing&SetUp-.*CablesCheckSuccess$',
                                r'.*&Setup.*AllCablesConnected.*-Next$',
                                r'MESSAGE-GatewayUnboxing&SetUp-AllCablesConnected',
                                r'MESSAGE-GatewayUnboxing&SetUp-TSBlinkingRedBridgeNotConnected-CablesCheck-Next',
                                r'SELECT_FLOW-GatewayUnboxing&SetUp-.*CableCheckStart-Yes.*find the cable',
                                ],

        'S2-CABLE_ERROR'      : [ 
                                r'MESSAGE-GatewayUnboxing&SetUp-.*-CheckCableConnection',
                                r'MESSAGE-GatewayUnboxing&SetUp-TSBlinkingRed-CheckCableConnection-Next',
                                r'MESSAGE-GatewayUnboxing&SetUp-TSBlinkingRed-BGW320505-MakeSure.*CableIsConnected-Next', 
                                r'MESSAGE-GatewayUnboxing&SetUp-TSBlinkingRed-Unplug.*ServiceBox-Next',
                                r'MESSAGE-GatewayUnboxing&SetUp-.{0,20}CablesNotConnected-BothCablesAreNotConnected(-Next|-Go|$)',
                                ],

        'S2-CABLE_AI'     : [ 
                            r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-AllCablesConnected-AICheckFJImageAnalysis',
                            r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-Default-H500PlugONTCableToSB',
                            r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-H500CablesCheckSuccess',
                            r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-H500CheckONTCable',
                            r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-H500CheckPowerCable',
                            r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-H500PlugONTCableToRG',
                            r'AR_MULTI_ELEMENT_GUIDANCE-GatewayUnboxing&SetUp.{1,50}CablesARGuidance',
                            r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-TS505CablesNotConnected',
                            ],

        'S2-MOVE_DEVICE'         : [
                                    r'.*&SetUp.*-MoveToFlatSurfaceSelection$',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSServiceLightSolidRed-MoveToFlatSurfaceSelection-Yes',
                                    ],

        'S2-MOVE_DEVICE-NO'         : [
                                    r'.*&SetUp.*-MoveToFlatSurfaceSelection-No',
                                    ],

        'S2-SETUP_DEVICE'       : [ 
                                    r'MESSAGE-GatewayUnboxing&SetUp.*-DeviceIsOverheating$',
                                    r'MESSAGE-GatewayUnboxing&SetUp.*-DeviceIsOverheating-Next',
                                    r'MESSAGE-GatewayUnboxing&SetUp-NowLetsSetUpYourDevice-Next', 
                                    r'MESSAGE-GatewayUnboxing&SetUp-DoNotPlaceObjectsOnTopOfYourRG',
                                    r'MESSAGE-GatewayUnboxing&SetUp-DoNotPlaceObjectsOnTopOfYourRG-Next', 
                                    r'WAIT_MESSAGE-GatewayUnboxing&SetUp-Timer',
                                    r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-NowLetsSetUpYourDevice', 
                                    r'LooksLikeYourDeviceIsTakingLonger',
                                    r'DeviceIsOverheating',
                                    r'WAIT_MESSAGE-GatewayUnboxing&SetUp-TSOtherColors-DeviceNotActivatedYetTimer',
                                    r'WAIT_MESSAGE-Locate&CheckServiceBox-TSONTPowerOff-Timer',
                                    r'WAIT_MESSAGE-GatewayUnboxing&SetUp-SetUp-Timer',
                                    r'.*GatewayUnboxing&SetUp.*-Timer',
                                    r'WAIT_MESSAGE-GatewayUnboxing&SetUp.*-DeviceIsTakingLongerTimer',
                                    r'.*&SetUp.*-DeviceNotActivatedYetTime',
                                    r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-Timer',
                                    ],

        'S2_CHECK_DEVICE_AI'     : [ 
                                    r'SCAN-GatewayUnboxing&SetUp-TSBlinkingRed-ModelStickerScan',
                                    r'SCAN-GatewayUnboxing&SetUp-TSLightsOff-ModelStickerScan',
                                ],

        'S2-LED_CHECK'  : [
                            r'.*&SetUp-.*-(Check|Manual)LEDLight$',
                            r'.*&SetUp-TSOtherColors-LEDCheck-CheckLEDLight-Go back',
                            r'.*&SetUp-TSOtherColors-LEDCheck-CheckLEDLight-Next',
                            r'.*&SetUp-TSOtherColors-LEDCheck-Next',
                            r'.*&SetUp-TSOtherColors-LEDCheckRecheck-CheckLEDLight-Go back',
                            r'.*&SetUp-TSOtherColors-LEDCheckRecheck-CheckLEDLight-Next',
                            r'.*&SetUp-TSOtherColors-ManualLEDCheck-Go back',
                            r'.*&SetUp-TSOtherColors-NoCamera-ManualLEDCheck-Go back',
                            r'.*&SetUp-TSOtherColors-NoCamera-ManualLEDCheck-Recheck-Go back',
                            r'.*&SetUp-TSOtherColors-NoCamera-ManualLEDCheck-Recheck-Next',
                            r'.*&SetUp-TSOtherColorsLEDCheck-CheckLEDLight-Go back',
                            r'.*&SetUp-TSOtherColorsLEDCheck-CheckLEDLight-Next',
                            r'.*&SetUp-TSOtherColorsLEDCheckRecheck-CheckLEDLight-Go back',
                            r'.*&SetUp-TSOtherColorsLEDCheckRecheck-CheckLEDLight-Next',
                            r'API_CALL-.{0,8}GatewayUnboxing&SetUp-BlinkingGreen-GetToken',
                            r'API_CALL-.{0,8}GatewayUnboxing&SetUp-BlinkingGreen-SendFeedback',
                            r'API_CALL-GatewayUnboxing&SetUp.*-LEDCheck$',
                            r'MESSAGE-GatewayUnboxing&SetUp.{1,20}(On|Off){0,1}(Light|Red|White|Green|Led)+(On|Off){0,1}',
                            r'MESSAGE-GatewayUnboxing&SetUp-.{1,40}-ManualLEDCheck-Next',
                            r'.*&SetUp-.*-(Check|Manual)LED.*-(Next|Go)',
                            # r'SELECT_FLOW-GatewayUnboxing&SetUp-TSServiceLightSolidRed-MoveToFlatSurfaceSelection-Yes',
                            r'SELECT_FLOW-GatewayUnboxing&SetUp-TSBlinkingRed-CheckONTGreenLight',
                            r'SELECT_FLOW-GatewayUnboxing&SetUp-TSBlinkingRed-CheckONTGreenLightRecheckSelection',
                            r'SELECT_FLOW-GatewayUnboxing&SetUp-TSBlinkingRedBridgeNotConnected-BridgeCheck-Yes, I did',
                            r'SELECT_FLOW-GatewayUnboxing&SetUp-TSBlinkingRed-EthJackConnection-Yes, I do',
                            r'SELECT_FLOW-check ONT green light 3GatewayUnboxing&SetUp-TSBlinkingRed-CheckONTGreenLightRecheckSelection$',
                                ],

        'S2-LED_AI'     : [ 
                            r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-LEDCheck',
                            r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-AllCablesConnected-LED',
                            r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-BeCloseToServiceBox',
                            r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-FrontxBack',
                            r'AR_MULTI_ELEMENT_GUIDANCE-GatewayUnboxing&SetUp.{1,50}OffARGuidance',
                            r'AR_MULTI_ELEMENT_GUIDANCE-GatewayUnboxing&SetUp.{1,50}OnARGuidance',
                            r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-Step2',
                            r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-OpenBox',
                            r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-.{1,50}-LEDCheck',
                            r'TAKE_IMAGE-GatewayUnboxing&SetUp-.{0,3}LEDCheckInstructions'
        #                     ],
        # 'S2-LED_AI_TS'     :[
                            r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-TSLightsOff',
                            r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-TSLightsOff-LEDCheck',
                            r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-TSBlinkingRed',
                            r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-TSFastRed-LEDCheck',
                            r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-TSLightsOff-PowerOutletLEDCheck',
                            r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-TSOtherColors-LEDCheck',
                            r'AR_MULTI_ELEMENT_GUIDANCE-GatewayUnboxing&SetUp-TS500CablesNotConnected-PowerNotConnectedARGuidanceRecheck',
                            r'AR_MULTI_ELEMENT_GUIDANCE-GatewayUnboxing&SetUp-TSBlinkingRed-BGW320500-ONTNotConnectedARGuidance',
                            r'LOGICAL_PROCESSOR-GatewayUnboxing&SetUp-.*LEDCheckResult$',
                            r'LOGICAL_PROCESSOR-GatewayUnboxing&SetUp-.*LEDCheckResultRecheck$',
                            r'LOGICAL_PROCESSOR-GatewayUnboxing&SetUp.*(LED|Light).*',
                            r'TEXT_PROCESSOR-GatewayUnboxing&SetUp-TSBlinkingRed-TextProcessor',
                            r'TEXT_PROCESSOR-GatewayUnboxing&SetUp-TSLightsOff-TextProcessor',                            
                            ],

        # 'S2_ALL_CABLES_OK'         : [
        #                         ],

        'S2-LED_ERROR'     : [ 
                                    r'API_CALL-.{0,8}GatewayUnboxing&SetUp-.{0,8}BlinkingRed-GetToken',
                                    r'API_CALL-.{0,8}GatewayUnboxing&SetUp-.{0,8}BlinkingRed-SendFeedback',
                                    r'API_CALL-.{0,8}GatewayUnboxing&SetUp-.{0,8}BlinkingRedBridgeNotConnected',
                                    r'API_CALL-APIGatewayUnboxing&SetUp-.{0,8}FastRed',
                                    r'API_CALL-APIGatewayUnboxing&SetUp-.{0,8}LightsOff',
                                    r'API_CALL-APIGatewayUnboxing&SetUp-TSOtherColors',
                                    r'API_CALL-APIGatewayUnboxing&SetUp-TSServiceLightSolidRed',
                                    r'API_CALL-APIGatewayUnboxing&SetUp-TSWPSLightRed',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSBlinkingRed-CheckONTGreenLight-No',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSBlinkingRed-(CheckONTGreenLightRecheckSelection-No|TryAgainSelection-Yes)',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSBlinkingRed-CheckONTGreenLightRecheckSelection-Yes.*red',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-NoCamera-ManualLEDCheck-Red(LED-Yes){0,1}',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSBlinkingRed-TryAgainSelection-No',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSBlinkingRed-NoCamera-ManualLEDCheck-Red',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSBlinkingRed-NoCamera-ManualLEDCheck-{2,20}Red',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSBlinkingRed-NoCamera-ManualLEDCheck-WhiteLED-Red',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSOtherColors-.{0,20}ManualLEDCheck-Red',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-.{0,30}ManualLEDCheck-Red',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSWPSLightRed.*-No',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSBlinkingRed.*-No',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSWPSLightRed.*-Yes.*red',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSBlinkingRed.*-Yes.*red',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSWPSLightRed.*-.*off',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSBlinkingRed.*-.*off',
                                    r'.*&Setup.*Failed.*ManualLEDCheck',
                                    r'.*&SetUp-.*-Lights Off.*',
                                    r'.*&SetUp-.*-Red.*',
                                    r'.*&SetUp-.*-Other Color.*',
                                    r'.*&SetUp.*-LightBlinkingRed.*',
                                    r'.*&SetUp-TSLightsOff.*-Light Off',
                                    r'.*&SetUp-TSOtherColors.*-Light Off',
                                    r'.*&SetUp-TSLightsOff-PowerOutletCheckPowerGreenLightSelection-No',
                                    r'.*&SetUp-.*-(Check|Manual)LED.*-Yes.*(blink|solid).*red',
                                    r'.*&SetUp-.*-(Check|Manual)LED.*-Light Off',
                                    # r'.*&SetUp-TSLightsOff-NoCamera-ManualLEDCheck-Light Off',
                                    r"SELECT_FLOW-InstallationTSBlinkingRedNoCameraManualLEDRed-No, it is solid red",
                                    r"SELECT_FLOW-InstallationTSBlinkingRedNoCameraManualLEDRed-Yes, it is blinking red",
                                    r"SELECT_FLOW-InstallationTSOtherColorsNoCameraManualLEDRed-No, it is solid red"  ,
                                    r"SELECT_FLOW-InstallationTSOtherColorsNoCameraManualLEDRed-Yes, it is blinking red",
                                    r"SELECT_FLOW-InstallationTSSolidRedNoCameraManualLEDRed-Yes, it is blinking red"    ,
                                    r"SELECT_FLOW-InstallationTSWPSLightRedNoCameraManualLEDRed-Yes, it is blinking red"  ,
                                    r"SELECT_FLOW-GatewayUnboxing&SetUp-TSSolidRed-NoCamera-ManualLEDCheck-Light Off",
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSBlinkingRedBridgeNotConnected-CheckFiberBridge',
                                ],

        'S2-LED_OK'         : [
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSLightsOff-CheckPowerGreenLightSelection-Yes',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSBlinkingRed-CheckONTGreenLight-Yes',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-NoCamera-ManualLEDCheck-White',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSOtherColors-LEDCheck-LightBlinkingWhite',
                                    r'MESSAGE-GatewayUnboxing&SetUp-TSOtherColors-NeedCameraAccess-Self-check',
                                    r'MESSAGE-GatewayUnboxing&SetUp-LEDCheck-NeedCameraAccess',
                                    r'MESSAGE-GatewayUnboxing&SetUp-NoCamera-ManualLEDCheck',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSOtherColors-ManualLEDCheck-White',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSOtherColors-NoCamera-ManualLEDCheck-White',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSLightsOff-NoCamera-ManualLEDCheck-White',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSBlinkingRed-.{0,20}ManualLEDCheck-White',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-TSBlinkingRed-CheckONTGreenLightRecheckSelection-Yes',
                                    # r'.*&SetUp.*-White.*',
                                    # r'.*&SetUp.*-(LED|Light).*Green.*',
                                    r'.*&SetUp.*-LightBlinkingWhite.*',
                                    r'SELECT_FLOW-Locate&CheckServiceBox-HappyPath-ONTIsWorking-Both lights are green',
                                    r'SELECT_FLOW-GatewayUnboxing&SetUp-FrontxBack-Both lights are green',
                                    ],

        'S2-SUCCESS'         : [ r'MESSAGE-AccountSetUp-RegisteredStep2',
                                r'API_CALL-GatewayUnboxing&SetUp-PreSHMHandover',
                                r'API_CALL-.{0,8}GatewayUnboxing&SetUp-GetToken',
                                r'API_CALL-.{0,8}GatewayUnboxing&SetUp-SendFeedback',
                                r'AR_MULTI_ELEMENT_GUIDANCE-GatewayUnboxing&SetUp.{1,50}SuccessARGuidance',
                                r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp-NoCamera-ManualLEDCheck-WhiteLED',
                                r'MESSAGE-APILocate&CheckServiceBox-GetToken-Next',
                                r'MESSAGE-GatewayUnboxing&SetUp-Success-Next',
                                r'MESSAGE-GatewayUnboxing&SetUp-.{1,50}AllCablesConnected',
                                r'.*GatewayUnboxing&SetUp-Success',
                                ],


        # ----------------------------------------------------------------------------------------------------

        'S3-START'              : [ 
                                r'MESSAGE-Activate&Manage-Step2',
                                r'MESSAGE-Activate&Manage-Step3',
                                r'MESSAGE-Activate&Manage-Step',
                                r'API_CALL-Activate&Manage$',
                                r'(API_CALL|MESSAGE)-.*Activate&Manage-GetToken$',
                                r'(API_CALL|MESSAGE)-.*Activate&Manage-GetToken-(Next|Go)',
                                r'MESSAGE-.*Activate&Manage-CheckRegistration-(Next|Go)',
                                r'API_CALL-Activate&Manage-SendFeedback',
                                r'LOGICAL_PROCESSOR-Activate&Manage-CheckRegistration',
                                    ],

        'S3-VOIP'               :[
                                r'MESSAGE-Activate&Manage-CheckVOIP-Go back',
                                r'MESSAGE-Activate&Manage-CheckVOIP-LogicalProcessor-Next',
                                r'.*-Activate.*VOIP',
                                r'MESSAGE-Activate&Manage-CheckVOIP-LogicalProcessor-Next',
                                r'SELECT_FLOW-Activate&Manage-CheckVOIP-Yes, I have my install kit',
                                ],

        'S3-SUCCESS'            : [ 
                                    r'MESSAGE-Activate&Manage-EndMessage',
                                    r'API_CALL-Activate&Manage-RegisteredEndMessage',
                                    ],

        'S3-REGISTERED'              : [ r'MESSAGE-Activate&Manage-Registered',
                                        r'MESSAGE-Activate&Manage-GuestModeRegistered',
                                        r'MESSAGE-Activate&Manage-AuthenticatedRegistered',
                                    ]  ,

        'S3-NOT_REGISTERED'              : [ 
                                            r'MESSAGE-Activate&Manage-NotRegistered',
                                            r'MESSAGE-Activate&Manage-AuthenticatedNotRegistered',
                                            r'MESSAGE-Activate&Manage-GuestModeNotRegistered',
                                    ]  ,

# -----------------------------------------------------------------------------------------------------------



        'CHECK_CE'           :[
                                r'.*&SetUp.*CheckCEData.*',
        #                         ],
        # 'S1-CHECK_CE'         : [ 
                                r'LOGICAL_PROCESSOR-Locate&CheckServiceBox-CheckCEData',
                                r'.*Locate.*CheckCEData.*',
                                r'(?i)(?!.*LogicalProcessor|Logical_Processor).*CheckCustomerStatus.*-(GetToken|GetStatus)',
                                r'MESSAGE-CheckCustomerStatus-LogicalProcessor-Next',
                                r'SELECT_FLOW-CheckCustomerStatus-LogicalProcessor-Yes, I did',
                                ],

        'SELFCHECK': [
                                    r'.*Locate&.*Self.*Check$',
                                    r'.*Locate&.*Manual.*Check',
        #                 ],
        # 'SELFCHECK': [
                                    r'.*SetUp.*Self.*Check$',
                                    r'.*SetUp.*Manual.*Check',
                        ],

        'NEEDCAMERA':       [
                                    r'.*Locate&.*NeedCameraAccess.*',
        #                 ],
        # 'NEEDCAMERA': [
                                    r'.*SetUp.*NeedCameraAccess.*',
        #                 ],
        # 'NO-CAMERA':                [
                                    r'.*Locate.*-NoCamera',
                                ],

        'S1-TAKE-PHOTO':        [
                                    r'.*(IMAGE)*.*Locate.*Photo',
                                ],
        'S2-TAKE-PHOTO':                [
                                    r'.*(IMAGE)*.*&SetUp.*Photo',
                                    r'.*&SetUp.*Take.*Picture',
                                ],

        'PROCESSOR': [  
        #             ],
        # 'S1-LOGICAL_PROCESSOR'  : [
                        r'IMAGE_ANALYSIS-GatewayUnboxing&SetUp.*-InstallationAnalysis$',
                        r'LOGICAL_PROCESSOR-Locate&CheckFJ', 
                        r'LOGICAL_PROCESSOR-Locate&CheckServiceBox',
                        r'LOGICAL_PROCESSOR-LocateFJ',
        #                         ],
        # 'S2-LOGICAL_PROCESSOR'  : [

        #                         ],
        # 'S3-LOGICAL_PROCESSOR'  : [
                        r'LOGICAL_PROCESSOR-Activate&Manage',
                        r'ARITHMETICAL_PROCESSOR-Arithmetical Processor',
                        r'LOGICAL_PROCESSOR',
                    ],

        'IMAGE_ANALYSIS': [
                        r'TAKE_IMAGE-Locate&CheckServiceBox-KeepSearching-LEDCheckInstructions',
                        r'IMAGE_ANALYSIS-APILocate&CheckServiceBox',
                        r'IMAGE_ANALYSIS-APILocateFJ',
                        r'IMAGE_ANALYSIS-Locate&CheckServiceBox-KeepSearching-LEDCheckInstructions',                        
                        r'IMAGE_ANALYSIS-Locate.*-Classification',
                        r'IMAGE_ANALYSIS-Locate.*-NoCamera-NeedFind(SB|FJ)$',            
                        r'IMAGE_ANALYSIS-',
                        r'TAKE_IMAGE-',
                        ],

        'API_CALL': [
                        r'API_CALL-.{0,20}Locate&CheckServiceBox-GetToken', 
                        r'API_CALL-Locate&CheckServiceBox-SendFeedback',
                        r'API_CALL-APILocateFJ',
                        r'API_CALL-LocateFJ',
                        r'API_CALL-APILocateFJ-TSCantFindFJ',
                        r'API_CALL-LocateFJTSCantFindFJ',
                        r'API_CALL',
                    ],

    }

    mapped_column = None
    for key, regex_list in regex_dict.items():
        condition = None
        for regex in regex_list:
            # Create a condition to check regex matches
            match_condition = F.trim(F.lower(column)).rlike(regex.lower())
            condition = match_condition if condition is None else condition | match_condition
        
        # Chain the `when` condition
        if mapped_column is None:
            mapped_column = F.when(condition, key)
        else:
            mapped_column = mapped_column.when(condition, key)

    return regex_dict, mapped_column


def check_regex_dict(reg):
    import re

    regex_dict_, _ = regex_dict()

    for k,v in regex_dict_.items():
        for l in v:
            if re.search(l, reg):
                print (f'{k:50s}', l)


@F.udf(T.ArrayType(T.IntegerType()))
def ai(hi, ty, el):
    total_elapsed = 0
    n = len(hi)
    i = 0
    ret = [0] * n

    while i < n:
        if not hi[i]:  # Start of a False group
            start = i
            # Find the end of this False group
            while i < n and not hi[i]:
                i += 1
            end = i - 1
            
            # Check if any ty in this group has "IMAGE"
            has_image = any("IMAGE_ANALYSIS" in ty[j] and \
                            (("LOGICAL_PROCESSOR" in ty[j:end + 1]) or \
                            ("ARITHMETICAL_PROCESSOR" in ty[j:end + 1]) or \
                            ("API_CALL" in ty[j:end + 1]) or \
                            ("MESSAGE" in ty[j:end + 1]) or \
                            ("SELECT_FLOW" in ty[j:end + 1]) or \
                            ( len(ty[j:end + 1])==1)  \
                            ) \
                            for j in range(start, end + 1))

            if has_image:
                for k in range(start, end +1):
                    ret[k] = el[k]
        else:
            i += 1

    return ret if ret and len(ret)>0 else None


@F.udf(T.MapType(T.StringType(), T.MapType(T.StringType(), T.IntegerType())))
def extract_image_info_data (coli, cole):
    from collections import defaultdict

    # Create the defaultdict with the specified initial keys
    def l(): return {'count': 0, 'sum': 0, 'avg': 0}
    ret = defaultdict(l)

    start = -1
    end = -1

    try:
        for i, val in enumerate(coli):
            val = np.double(val)
                
            if not np.isinf(val):
                if  val ==  -1: 
                    ret['_1']['count'] += 1
                    ret['_1']['sum'] += cole[i]
                    ret['_1']['avg'] = ret['_1']['sum'] / ret['_1']['count']
                if  val ==  0: 
                    ret['0']['count'] += 1
                    ret['0']['sum'] += cole[i]
                    ret['0']['avg'] = ret['0']['sum'] / ret['0']['count']
                if  val ==  1: 
                    ret['1']['count'] += 1
                    ret['1']['sum'] += cole[i]
                    ret['1']['avg'] = ret['1']['sum'] / ret['1']['count']
                if val == 10 or val == 20 or val == 30: 
                    ret['10']['count'] += 1
                    ret['10']['sum'] += cole[i]
                    ret['10']['avg'] = ret['10']['sum'] / ret['10']['count']
                    
                    # start = -1

        for e in ret.values():
            if e['count']>0:
                return dict(ret)
    except:
        pass

    return None



import numpy as np

@F.udf(returnType=T.IntegerType(), )
def sum_total_captured_images (x):  
    acc=0
    if len(x) == 0: return None

    for r in x:
        if r>0:
            acc=r+acc
    return acc






def preprocess(df):
    
    df = (
        df
        .withColumn('total_captured_images',
                    F.when( F.size('total_captured_images') == 0 , None)
                    .otherwise(F.col('total_captured_images'))
        )
        .withColumn('ai_elapsed', 
                    ai(F.col('human_interaction'), F.col('type'), F.col('elapsed'))
        )
        .withColumn('image_count', 
                    extract_image_info_data(F.col('total_captured_images'), F.col('ai_elapsed'))
                    )
        .withColumn('sum_ai_elapsed',
                    F.aggregate(
                        F.col('ai_elapsed'),
                        F.lit(0),
                        lambda acc, x: x+acc
                    )
        )
        .withColumn('sum_total_captured_images',
                    sum_total_captured_images(  F.col('total_captured_images')   )
        )            
        .cache()
        .withColumn('CALL_RSLT_IN_DISPATCH',
                    F
                    .when( F.col('CALL_RSLT_IN_DISPATCH').cast('string').rlike('.*(Y|1)'), 1 )
                    .when( F.col('CALL_RSLT_IN_DISPATCH').cast('string').rlike('.*(N|0)'), 0 )
                    .otherwise(None)
                    )
        .withColumn('RPT_CALL_DAY7',
                    F
                    .when( F.col('RPT_CALL_DAY7').cast('string').rlike('.*(Y|1)'), 1 )
                    .when( F.col('RPT_CALL_DAY7').cast('string').rlike('.*(N|0)'), 0 )
                    .otherwise(None)
                    )
        .withColumn('AB_test', 
                        F
                        .when( F.col('customer_id').rlike(r'.*5$')       , 'A')
                        .when( F.col('customer_id').rlike(r'.*(3|7|9)$') , 'B')
                        .otherwise(None)
                    )
        .select(
            '*', 
            # 'image_count', 
            F.col('image_count._1.count').alias('image__1_count'),     ##  IMAGE for some reason is null and/or image was not detected in device_info_device_name
            F.col('image_count._1.sum').alias('image__1_sum'), 
            F.col('image_count._1.avg').alias('image__1_avg'), 

            F.col('image_count.0.count').alias('image_0_count'),       ##  IMAGE for some reason is not null BUT junk/wall device_info_device_name
            F.col('image_count.0.sum').alias('image_0_sum'), 
            F.col('image_count.0.avg').alias('image_0_avg'), 

            F.col('image_count.1.count').alias('image_1_count'),       ##  IMAGE detected and 1 correctly found
            F.col('image_count.1.sum').alias('image_1_sum'), 
            F.col('image_count.1.avg').alias('image_1_avg'), 

            F.col('image_count.10.count').alias('image_10_count'),     ##  IMAGE detected and 10 correctly found
            F.col('image_count.10.sum').alias('image_10_sum'), 
            F.col('image_count.10.avg').alias('image_10_avg'), 

            # F.col('image_count.20.count').alias('image_20_count'),     ##  IMAGE detected and 20 correctly found
            # F.col('image_count.20.sum').alias('image_20_sum'),
            # F.col('image_count.20.avg').alias('image_20_avg'), 
            )
        .drop(*['event_rank', 'rank', 'min_rank'])
        # .find()
        .cache()
    )

    regex_dict_, mapped_column = regex_dict()

    df_combined = df.withColumn("zipped", 
                                F.arrays_zip(
                                    F.col("activity_details"),
                                    F.col("activity_grouping"), 
                                    F.col("activity"), 
                                    F.col("elapsed")
                                    ))

    # Explode the combined column
    df_exploded = df_combined.select(*[
        F.col('customer_id'), 
        F.col('session_id'),
        F.posexplode(F.col("zipped")).alias("rank", "exploded")
        ])

    # Extract individual columns back from the struct
    df_final = df_exploded.select(
        F.col("customer_id"),
        F.col("session_id"),
        F.col("rank"),
        F.col("exploded.activity_details").alias("activity_details"),
        F.col("exploded.activity_grouping").alias("activity_grouping"),
        F.col("exploded.activity").alias("activity"),
        F.col("exploded.elapsed").alias("elapsed"),
    )\
    .join(
        df.select(*['customer_id', 'session_id', 
                    # 'step', 'label', 
                    'SESSION_START_TS', 'SESSION_END_TS', 
                    # 'customer_journey', 'DISPATCH_COUNT', 'CALL_RSLT_IN_DISPATCH', 'RPT_CALL_DAY7', 'AB_test',
                    ]),
        on = ['customer_id', 'session_id'],
        how = 'left'
    )

    window_spec = Window.partitionBy("customer_id", 'session_id').rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # Calculate the running elapsed time
    df_final = df_final\
        .withColumn("running_elapsed_time", F.sum("elapsed").over(window_spec))\
        .withColumn(
            "updated_time",
                F.to_timestamp( F.from_unixtime(
                F.unix_timestamp(F.col("SESSION_START_TS")) + F.col("running_elapsed_time")
            ) )
    )

    # Apply the cumulative condition and set a default value as `null`
    df_final = df_final.withColumn("mapped_key", mapped_column.otherwise(F.col('activity')))\
                .cache()

    dg       = df_final.filter( ~( F.col('mapped_key').isin(list(regex_dict_.keys())) ) )\
                .cache()

    df_final = df_final.filter( ( F.col('mapped_key').isin(list(regex_dict_.keys())) ) )\
                .cache()

    window = W.Window.partitionBy(['customer_id', 'session_id']).orderBy('rank')

    df_final = (
                df_final
                .withColumn('next', F.lag('mapped_key', -1).over( window ) )
                .withColumn('updated_time', 
                    F.when( F.col('rank') == 0, F.to_timestamp( F.unix_timestamp( F.col('SESSION_START_TS') ) ))
                    .otherwise(F.col('updated_time')) )
                .repartition(10, ['customer_id', 'rank'])
                .sort(['customer_id', 'rank'])
                .cache()
                .withColumn(
                    "group_change",
                    F.when(
                        (F.col("mapped_key") != F.col("next")) | F.col("next").isNull(),
                        1
                    ).otherwise(0)
                )
                .withColumn(
                    "group_id",
                    F.sum("group_change").over( window.rowsBetween(Window.unboundedPreceding, -1))
                )
                .fillna('--END--', subset='next')
                .fillna(0, subset='group_id')
    )

    S3START = ['S3-START',]
    S1COMPLETE = ['S1-FINISH',]
    COMPLETES = ['S3-NOT_REGISTERED', 'S3-REGISTERED', 'S3-SUCCESS' ] # + S1COMPLETE
    SERVICE_TRANSFER = ['S1-SERVICE_TRANSFER', 'S2-SERVICE_TRANSFER', 'SERVICE_TRANSFER']

    # Define priority-based ordering for the window
    window_spec = Window.partitionBy("session_id").orderBy(
        F
        .when(F.col("label") == "COMPLETE", 1)
        .when(F.col("label") == "SERVICE", 2)
        .when(F.col("label") == "INCOMPLETE", 3)
        .otherwise(4)  # None values are last
    ).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    result = (df_final
                .value_counts(
                    ['customer_id', 'session_id', 'SESSION_START_TS', 'SESSION_END_TS', 'mapped_key', 'group_id'], 
                    agg=[ 
                    F.min('rank').alias('rank'), 
                    F.sum('elapsed').alias('elapsed'), 
                    F.max('updated_time').alias('updated_time'), 
                    F.collect_list('activity').alias('activity'), 
                ])
                .withColumnRenamed('counts', 'screen_grouped_count')
                .withColumn('next', F.lag('mapped_key', -1).over( window ) )
                .fillna('--END--', subset='next')
                .withColumn('label',
                        F
                        .when( F.col('mapped_key').isin(SERVICE_TRANSFER) , 'SERVICE_TRANSFER' )
                        # .when( F.col('mapped_key').isin(S1COMPLETE)        , 'S1'  )
                        .when(  ( F.col('mapped_key').isin(COMPLETES) )  |
                                ( F.col('mapped_key').isin(S3START) & F.col('next').isin(S1COMPLETE) )
                                    , 'COMPLETE'  )
                        .when(  (~F.col('mapped_key').isin(COMPLETES) ) &
                                (~F.col('mapped_key').isin(SERVICE_TRANSFER) ) &
                                (F.col('next').isin(['--END--']) )
                                                                , 'INCOMPLETE'  
                            )
                        .otherwise(None)
                        )
                .withColumn('label',
                            F.first(F.col("label"), ignorenulls=True).over(window_spec)
                        )
                .drop(*[
                    'group_id',	'screen_grouped_count',
                ])

                .sort(['customer_id', 'session_id', 'rank'])
                .groupBy([
                    'customer_id',    'session_id',                                	
                    'SESSION_START_TS',                               	
                    'SESSION_END_TS',       
                    'label',                          	
                    ])
                .agg(*[
                        F.collect_list('mapped_key').alias('mapped_key'),
                        F.collect_list('next').alias('next'),
                        F.collect_list('elapsed').alias('mapped_elapsed'),
                        F.collect_list('updated_time').alias('updated_time'),
                        F.collect_list('activity').alias('mapped_activity'),
                ])
                .join(
                    df.drop_duplicates(
                        subset=['customer_id', 'session_id', 'SESSION_START_TS', 'SESSION_END_TS',]
                        )
                        .drop(*[
                            'SESSION_START_TS', 'SESSION_END_TS', 'mapped_key', 'next', 'group_id', 
                            'rank', 'label', 
                            'step', 'step1', 'step123', 'step23',
                            'updated_time'
                                    ])
                        ,
                    on = ['customer_id', 'session_id'],
                    how = 'left'
                )
    )

    return result, dg




