
# Billing/Service/Sales Pattern
bb_ss_pattern = r'cable package|phone unlocked|landline bill|billing.{0,40}go up|bill.{0,15}high|put.{0,15}billing|credit|promotion|payment|pay|billing.{0,15}department|saldo.{0,15}vencido|another.{0,15}service|' \
r'paperless|bill.{0,15}too high|discount|descuento|lower.{0,15}bill|due|balance|my.{0,15}bill|late fee.{0,15}bill|fee.{0,15}bill|a t and t account|new.{0,15}account|activate equipment|obtener.{0,15}dispositivo nuevo|' \
r'expensive|dispute.{0,15}bill|seasonal suspension|switching ownership|status.{0,15}account|pagando.{0,15}mucho.{0,15}servicio|activate esim|tengo que pagar|caro|factura|' \
r'downgrading service|account.{0,15}on hold|phone.{0,15}bill|suspended.{0,15}account|reconnect.{0,15}service|internet.{0,15}reactivated|get.{0,15}data offer|add a line|' \
r'name change|security breach|privacy.{0,15}account|vacation|pause.{0,15}service|restore.{0,15}phone|incorrect bill|afford|account.{0,15}issues.{0,15}billing|charge.{0,45}has increased|' \
r'down.{0,25}so.{0,25}much.{0,25}money|billing|paper bill|pagar.{0,30}caros|billing.{0,15}explanation of charges|cobro de mas|arreglo de pago|over charge.{0,15}bill|hacer un pago|pago|' \
r'upgrade.{0,15}plan|plan.{0,15}change|monthly.{0,15}fee|refund|reembolso|autopay|auto.{0,15}pay|late.{0,15}payment|overdue|bill.{0,15}adjustment|billing.{0,15}error|' \
r'invoice|statement|account.{0,15}summary|service.{0,15}charge|equipment.{0,15}fee|activation.{0,15}fee|termination.{0,15}fee|early.{0,15}termination|contract|contrato|' \
r'plan.{0,15}recommendation|bundle|paquete|savings|ahorros|promo.{0,15}code|special.{0,15}offer|limited.{0,15}time|price.{0,15}increase|rate.{0,15}change|tarifa'

# Loyalty/Retention/Cancellation Pattern
bb_loyalty_pattern = "|".join([
    "cancel", "canceling", "cancellation", "loyalty", "disconnect", "disconnected", "disconnecting", "reward",
    "rewards", "points", "withdrawal", "gift", "move", "moving", "close", "service transfer", "transfer service", "transfer",
    "allegiance", "commit", "cancel service", "change", "retention", "retain", "keep", "stay", "leaving", "switch",
    "dedication", "devotion", "fidelity", "faithfulness", "fealty", "competitor", "competition", "better deal",
    "steadfastness", "attachment", "adhesion", "faith", "constancy", "unhappy", "dissatisfied", "frustrated",
    "piety", "devotedness", "affection", "appreciation", "grateful", "thankful", "satisfied", "happy",
    "fastness", "troth", "fondness", "dependability", "determination", "reliability", "trust", "confidence",
    "trustworthiness", "resolution", "firmness", "trustability", "trustiness", "cancelar", "desconectar",
    "mudanza", "cambio", "transferir", "cerrar", "terminar", "finalizar", "competencia", "mejor precio",
    "insatisfecho", "molesto", "agradecido", "contento", "satisfecho", "confianza", "upgrading", "downgrading"
]) + \
r'|cancel.{1,15}service|move.{1,15}service|different.{1,15}location|downgrade.{1,15}plan|down.{1,15}service' \
r'different.{1,15}address|estatus.{1,15}pedido|new service|changing.{1,15}service|' \
r'end.{1,15}service|terminate.{1,15}account|close.{1,15}account|port.{1,15}number|' \
r'switch.{1,15}provider|other.{1,15}company|competitor.{1,15}offer|better.{1,15}deal|' \
r'customer.{1,15}retention|save.{1,15}account|win.{1,15}back|special.{1,15}discount|' \
r'loyalty.{1,15}program|tenure.{1,15}discount|long.{1,15}time.{1,15}customer'

# Technical Support Pattern
bb_tech_pattern = r'install|troubleshoot|technician|need.{0,15}appointment|broken line|internet.{0,15}slow|tech.{0,15}support|internet not work|' \
r'cable service|installation|service.{0,15}repair|pending order|internet.{0,15}not working|landline not working|internet is out|' \
r'internet.{0,15}down|repair|internet setup|new router|problem.{0,20}fiber|wi-fi.{0,15}not working|soporte tecnico|internet service|' \
r'reboot.{0,15}wi-fi|problem.{0,15}my service|move.{0,15}router|no internet|modem installation|slow connec|tv streaming|email|direct tv|' \
r'cancel technician|internet.{0,15}setup|appointment|technical.{0,20}assistance|tech.{0,30}assist|cannot get wifi|hotspot cannot connect|' \
r'(set up|setup).{0,15}wifi|tech.{0,15}support|outage|(service|network|connectivity).{0,15}(interruption|failure|down|disrupt|issue|unavail)|' \
r'connection.{0,15}problem|signal.{0,15}weak|no.{0,15}signal|equipment.{0,15}malfunction|device.{0,15}not.{0,15}working|modem.{0,15}issue|' \
r'router.{0,15}problem|bandwidth.{0,15}issue|speed.{0,15}test|latency|ping|packet.{0,15}loss|dns.{0,15}issue|ip.{0,15}address|' \
r'firmware.{0,15}update|reset.{0,15}modem|reset.{0,15}router|power.{0,15}cycle|unplugged|cable.{0,15}loose|wiring.{0,15}issue|' \
r'line.{0,15}quality|static|noise|interference|weather.{0,15}related|storm.{0,15}damage|physical.{0,15}damage|' \
r'network.{0,15}configuration|port.{0,15}forwarding|firewall|security.{0,15}settings|wireless.{0,15}settings|ssid|password.{0,15}wifi|' \
r'intermittent.{0,15}connection|dropping.{0,15}connection|frequent.{0,15}disconnect|stability.{0,15}issue|' \
r'instalacion|tecnico|reparacion|cita|internet.{0,15}lento|no.{0,15}funciona|problema.{0,15}internet|sin.{0,15}internet|' \
r'configuracion|reiniciar|restablecer|conexion.{0,15}intermitente|velocidad.{0,15}lenta'

# Greeting Pattern
bb_greeting_pattern = r'hello|hi|good morning|good afternoon|good evening|thank you for calling|thanks for calling|' \
r'welcome|greetings|how are you|how can I help|how may I assist|what can I do for you|' \
r'hola|buenos dias|buenas tardes|buenas noches|gracias por llamar|bienvenido|como esta|como puedo ayudar|' \
r'bonjour|bonsoir|comment allez|puis-je vous aider|' \
r'my name is|this is|speaking with|you.{0,10}reached|customer service|tech support|billing department'

# Closing Pattern  
bb_closing_pattern = r'thank you|thank|have a great day|have a good day|goodbye|bye|take care|anything else|' \
r'is there anything else|further assistance|additional questions|call back|feel free to call|' \
r'gracias|que tenga buen dia|que tengas buen dia|adios|hasta luego|cuidense|algo mas|' \
r'alguna otra pregunta|llamar de nuevo|' \
r'merci|au revoir|bonne journee|autre chose|' \
r'pleasure helping|glad to help|happy to assist|resolution|resolved|fixed|solved|completed|' \
r'satisfaction survey|feedback|rate your experience|survey|great.{1,5}day|good day'

# Additional pattern for general customer service inquiries
bb_general_pattern = r'customer.{0,15}service|help|assistance|question|inquiry|information|explain|understand|confused|' \
r'representative|agent|speak.{0,15}person|human|supervisor|manager|escalate|complaint|complain|' \
r'servicio.{0,15}cliente|ayuda|asistencia|pregunta|informacion|explicar|entender|confundido|' \
r'representante|agente|hablar.{0,15}persona|supervisor|gerente|queja|reclamar'

name_pattern = (
    r"(?:[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)"  # Matches first name or full name
)

NAME_REGEX = (
    r"(?i)"  
    r"(?:\b(?:hi|hello|hey|good\s(?:morning|afternoon|evening))?[\s,;:-]*)?"  # optional greeting
    r"(?:my\s+name\s+is|i(?:\s+am|['’]m)\s+called|this\s+is|i(?:\s+am|['’]m))\s+"
    rf"{name_pattern}"
    r"|(?:thank\s+you|you're\s+welcome|you\s+are\s+welcome)[\s,;:-]*"
    rf"{name_pattern}"
)