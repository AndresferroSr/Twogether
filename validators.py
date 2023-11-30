def validar_campos(request_form, *campos):
    for campo in campos:
        if campo not in request_form or not request_form[campo]:
            return False
    return True