def validar_campos(request_form, *campos):
    for campo in campos:
        if campo not in request_form or not request_form[campo]:
            return False
    return True


def correct_frame(frame):
    for data in frame.columns:
        frame[f"{data}"] =  frame[f"{data}"].astype(str)
    return frame


def obtener_primer_nombre(nombre_completo):
    return nombre_completo.split()[0]
