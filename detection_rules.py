def check_spo2(value, threshold=92):
    """Returns True if SpO2 is dangerously low."""
    return value < threshold


def check_temp(value, threshold=38.0):
    """Returns True if body temperature indicates fever."""
    return value > threshold


def check_heart_rate(value, low=50, high=120):
    """Returns True if heart rate is outside normal resting/active range."""
    return value < low or value > high


def is_anomalous(spo2, temp, heart_rate):
    """Combined check — True if ANY threshold rule fires."""
    return check_spo2(spo2) or check_temp(temp) or check_heart_rate(heart_rate)
