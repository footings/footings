def _generate_message(msg, params):
    if len(params) == 1:
        m_params = params[0]
    else:
        m_params = ", ".join(str(x) for x in params)
    return msg + "[" + m_params + "]"
